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

        var libraries = await LibraryStore.GetEnabledAsync();
        _logger.LogInformation(
            $"Crawling {libraries.Count} enabled libraries: {string.Join(", ", libraries.Select(l => l.Label))}");

        foreach (var library in libraries)
        {
            try
            {
                await CrawlLibrary(library);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Crawl failed for library {library.Label}");
            }
        }

        _logger.LogInformation("Crawler finished");
    }

    private async Task CrawlLibrary(LibraryEntry library)
    {
        _logger.LogInformation($"Crawling library '{library.Label}' ({library.DriveId})");

        // DeltaState is keyed per drive, so each library tracks its own incremental delta.
        string? deltaLink = await GetDeltaLinkAsync(library.DriveId);

        string? url = string.IsNullOrEmpty(deltaLink)
            ? $"https://graph.microsoft.com/v1.0/drives/{library.DriveId}/root/delta"
            : deltaLink;

        while (!string.IsNullOrEmpty(url))
        {
            var page = await GraphGet(url);

            if (page.TryGetProperty("value", out var items))
            {
                foreach (var item in items.EnumerateArray())
                {
                    // Deletions first — a removed item may no longer carry a "file" facet.
                    if (item.TryGetProperty("@removed", out _))
                    {
                        var removedId = item.TryGetProperty("id", out var idEl) ? idEl.GetString() : null;
                        if (!string.IsNullOrEmpty(removedId))
                            await DeleteFromIndex(removedId!);
                        continue;
                    }

                    // Skip folders / non-file items.
                    if (!item.TryGetProperty("file", out _))
                        continue;

                    await EnqueueFile(item, library);
                }
            }

            if (page.TryGetProperty("@odata.nextLink", out var next))
            {
                url = next.GetString();

                // Persist the in-progress cursor after every page so a throttled or interrupted
                // crawl RESUMES from here on the next tick instead of restarting the whole
                // library from scratch (the bug that re-enqueued the corpus every 10 minutes).
                if (!string.IsNullOrEmpty(url))
                    await SaveDeltaLinkAsync(library.DriveId, url!);
            }
            else if (page.TryGetProperty("@odata.deltaLink", out var delta))
            {
                var dl = delta.GetString();
                if (!string.IsNullOrEmpty(dl))
                    await SaveDeltaLinkAsync(library.DriveId, dl);

                break;
            }
            else
            {
                break;
            }
        }
    }

    private async Task EnqueueFile(JsonElement item, LibraryEntry library)
    {
        // Use TryGetProperty everywhere: some Graph delta items legitimately lack webUrl or
        // lastModifiedDateTime, and a single missing key must NOT throw and abort the whole
        // library crawl (which leaves the delta cursor stuck and stops all indexing).
        var itemId = item.TryGetProperty("id", out var idEl) ? idEl.GetString() : null;
        var fileName = item.TryGetProperty("name", out var nameEl) ? nameEl.GetString() : null;
        if (string.IsNullOrEmpty(itemId) || string.IsNullOrEmpty(fileName))
            return;

        var message = new
        {
            driveId = library.DriveId,
            library = library.Label,
            itemId,
            fileName,
            webUrl = item.TryGetProperty("webUrl", out var w) ? w.GetString() : null,
            lastModified = item.TryGetProperty("lastModifiedDateTime", out var lm) ? lm.GetString() : null
        };

        var payload = Convert.ToBase64String(
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(message)));

        await _queue.SendMessageAsync(payload);
    }

    // Remove every indexed chunk that belonged to a file deleted in SharePoint.
    // Chunk ids are tracked in the IndexRegistry table, so no index-side filtering is required.
    private async Task DeleteFromIndex(string itemId)
    {
        var searchIds = await IndexRegistry.GetSearchIdsAsync(itemId);
        if (searchIds.Count == 0)
        {
            _logger.LogInformation($"No tracked chunks for removed item {itemId}; nothing to delete.");
            return;
        }

        var body = new
        {
            value = searchIds.Select(id => new Dictionary<string, object>
            {
                ["@search.action"] = "delete",
                ["id"] = id
            }).ToArray()
        };

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{Env("SEARCH_SERVICE")}/indexes/{Env("SEARCH_INDEX")}/docs/index?api-version=2024-07-01");
        request.Headers.Add("api-key", Env("SEARCH_KEY"));
        request.Content = new StringContent(
            JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");

        using var response = await _http.SendAsync(request);

        if (response.IsSuccessStatusCode)
        {
            await IndexRegistry.RemoveAsync(itemId);
            _logger.LogInformation($"Deleted {searchIds.Count} indexed chunks for removed item {itemId}.");
        }
        else
        {
            var error = await response.Content.ReadAsStringAsync();
            _logger.LogError($"Failed to delete chunks for {itemId}: {response.StatusCode} {error}");
        }
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

    private TableClient GetDeltaTable() =>
        new TableClient(Env("AzureWebJobsStorage"), "DeltaState");

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
