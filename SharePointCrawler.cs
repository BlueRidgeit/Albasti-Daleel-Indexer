using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Azure.Storage.Queues;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Azure.Data.Tables;

public class SharePointCrawler
{
    private readonly ILogger<SharePointCrawler> _logger;
    private readonly HttpClient _http = new();
    private readonly QueueClient _queue;

    public SharePointCrawler(ILogger<SharePointCrawler> logger)
    {
        _logger = logger;

        _queue = new QueueClient(
            Env("AzureWebJobsStorage"),
            "spo-ingestion-queue");

        _queue.CreateIfNotExists();
    }

    [Function(nameof(SharePointCrawler))]
    public async Task Run([TimerTrigger("0 */10 * * * *")] TimerInfo timer)
    {
        _logger.LogInformation($"Crawler started @ {DateTime.UtcNow}");

        string token = await GetGraphToken();
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);

        string driveId = Env("DRIVE_ID");

        // ✅ Load last delta (if any)
        string? deltaLink = await GetDeltaLinkAsync(driveId);

        string url = string.IsNullOrEmpty(deltaLink)
            ? $"https://graph.microsoft.com/v1.0/drives/{driveId}/root/delta"
            : deltaLink;

        while (!string.IsNullOrEmpty(url))
        {
            var page = await GraphGet(url);

            if (page.TryGetProperty("value", out var items))
            {
                foreach (var item in items.EnumerateArray())
                {
                    // ✅ Skip non-files
                    if (!item.TryGetProperty("file", out _))
                        continue;

                    // ✅ Skip deletions (handled later in Phase 3.4)
                    if (item.TryGetProperty("@removed", out _))
                        continue;

                    await EnqueueFile(item);
                }
            }

            if (page.TryGetProperty("@odata.nextLink", out var next))
            {
                url = next.GetString();
            }
            
            else if (page.TryGetProperty("@odata.deltaLink", out var delta))
            {
                var dl = delta.GetString();
                if (!string.IsNullOrEmpty(dl))
                    await SaveDeltaLinkAsync(driveId, dl);

                break;
            }

            else
            {
                break;
            }
        }

        _logger.LogInformation("Crawler finished");
    }

    private async Task EnqueueFile(JsonElement item)
    {
        var message = new
        {
            driveId = Env("DRIVE_ID"),
            itemId = item.GetProperty("id").GetString(),
            fileName = item.GetProperty("name").GetString(),
            webUrl = item.GetProperty("webUrl").GetString(),
            lastModified = item.GetProperty("lastModifiedDateTime").GetString()
        };

        var payload = Convert.ToBase64String(
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message)));

        await _queue.SendMessageAsync(payload);
    }

    private async Task<JsonElement> GraphGet(string url)
    {
        var resp = await _http.GetAsync(url);

        if (!resp.IsSuccessStatusCode)
        {
            var error = await resp.Content.ReadAsStringAsync();
            _logger.LogError(
                "Graph call failed {StatusCode} for {Url}: {Error}",
                resp.StatusCode, url, error);

            throw new InvalidOperationException(
                $"Graph call failed: {resp.StatusCode}");
        }

        return JsonDocument
            .Parse(await resp.Content.ReadAsStringAsync())
            .RootElement;
    }

    private async Task<string> GetGraphToken()
    {
        var app = ConfidentialClientApplicationBuilder
            .Create(Env("CLIENT_ID"))
            .WithClientSecret(Env("CLIENT_SECRET"))
            .WithAuthority($"https://login.microsoftonline.com/{Env("TENANT_ID")}")
            .Build();

        var result = await app
            .AcquireTokenForClient(new[] { "https://graph.microsoft.com/.default" })
            .ExecuteAsync();

        return result.AccessToken;
    }

    private TableClient GetDeltaTable()
{
    return new TableClient(
        Env("AzureWebJobsStorage"),
        "DeltaState");
}

private async Task<string?> GetDeltaLinkAsync(string driveId)
{
    var table = GetDeltaTable();
    await table.CreateIfNotExistsAsync();

    try
    {
        var entity = await table.GetEntityAsync<TableEntity>("Drive", driveId);
        return entity.Value.GetString("DeltaLink");
    }
    catch
    {
        return null;
    }
}

    private async Task SaveDeltaLinkAsync(string driveId, string deltaLink)
    {
        var table = GetDeltaTable();
        await table.CreateIfNotExistsAsync();

        var entity = new TableEntity("Drive", driveId)
        {
            { "DeltaLink", deltaLink }
        };

        await table.UpsertEntityAsync(entity);
    }

    private static string Env(string key) =>
        Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"Missing setting: {key}");
}