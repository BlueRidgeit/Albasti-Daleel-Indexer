using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Azure.Storage.Queues;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

/// <summary>
/// Manual re-index. Walks one or more SharePoint document libraries and enqueues every file onto
/// the same ingestion queue the delta crawler uses, so the resilient SharePointWorker pipeline
/// (throttle handling + retry queue) does the extraction/embedding/indexing.
///
/// On demand via HTTP:
///   POST /api/reindex                      -> re-index all ENABLED libraries
///   POST /api/reindex?driveId=...          -> re-index a single library (label resolved from LibraryStore)
///   POST /api/reindex?driveId=...&library= -> re-index a single library with an explicit label
/// </summary>
public class SharePointIngestor
{
    private readonly ILogger<SharePointIngestor> _logger;
    private readonly HttpClient _http = new();
    private readonly QueueClient _queue;

    public SharePointIngestor(ILogger<SharePointIngestor> logger)
    {
        _logger = logger;
        _queue = new QueueClient(Env("AzureWebJobsStorage"), "spo-ingestion-queue");
        _queue.CreateIfNotExists();
    }

    [Function("FullReindex")]
    public async Task<IResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = "reindex")] HttpRequest req)
    {
        // Un-index path: remove a library from the crawl list and delete its indexed chunks.
        // No Graph access needed, so handle it before acquiring a token.
        if (string.Equals(req.Query["action"], "remove", StringComparison.OrdinalIgnoreCase))
        {
            string? removeDriveId = req.Query["driveId"];
            if (string.IsNullOrWhiteSpace(removeDriveId))
            {
                return Results.BadRequest(new { error = "driveId is required for remove." });
            }

            string? requestedLabel = req.Query["library"];
            var existing = await LibraryStore.GetAsync(removeDriveId);
            var label = !string.IsNullOrWhiteSpace(requestedLabel) ? requestedLabel : existing?.Label;

            await LibraryStore.DeleteAsync(removeDriveId);
            int deleted = !string.IsNullOrWhiteSpace(label)
                ? await DeleteLibraryChunks(label!)
                : 0;

            _logger.LogInformation($"Un-indexed '{label}' ({removeDriveId}): removed from crawl list, deleted {deleted} chunks.");
            return Results.Ok(new { removed = label, deletedChunks = deleted });
        }

        _logger.LogInformation($"Manual re-index started @ {DateTime.UtcNow}");

        string token = await GetGraphToken();
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);

        // Resolve which libraries to re-index.
        List<LibraryEntry> targets;
        string? driveId = req.Query["driveId"];
        if (!string.IsNullOrWhiteSpace(driveId))
        {
            string? label = req.Query["library"];
            var entry = await LibraryStore.GetAsync(driveId)
                        ?? new LibraryEntry(driveId!, string.IsNullOrWhiteSpace(label) ? driveId! : label!, true);
            // Register/enable this library so the scheduled crawler keeps it in sync going forward.
            await LibraryStore.UpsertAsync(entry.DriveId, entry.Label, true);
            targets = new List<LibraryEntry> { entry };
        }
        else
        {
            targets = await LibraryStore.GetEnabledAsync();
        }

        // Optional extension filter, e.g. ?ext=pdf re-enqueues only PDF files (used to re-OCR
        // PDFs with Document Intelligence without re-processing every Word document).
        string? extFilter = req.Query["ext"];

        int enqueued = 0;
        foreach (var library in targets)
        {
            enqueued += await EnqueueChildren(library, null, extFilter);
        }

        _logger.LogInformation(
            $"Re-index enqueued {enqueued} {(string.IsNullOrWhiteSpace(extFilter) ? "" : extFilter + " ")}files across {targets.Count} libraries.");

        return Results.Ok(new
        {
            libraries = targets.Select(t => t.Label),
            ext = string.IsNullOrWhiteSpace(extFilter) ? null : extFilter,
            enqueued
        });
    }

    // folderId == null => library root. extFilter (e.g. "pdf") limits which files are enqueued.
    private async Task<int> EnqueueChildren(LibraryEntry library, string? folderId, string? extFilter = null)
    {
        string? url = folderId is null
            ? $"https://graph.microsoft.com/v1.0/drives/{library.DriveId}/root/children"
            : $"https://graph.microsoft.com/v1.0/drives/{library.DriveId}/items/{folderId}/children";

        int count = 0;

        while (!string.IsNullOrEmpty(url))
        {
            var page = await GraphGet(url);

            foreach (var item in page.GetProperty("value").EnumerateArray())
            {
                if (item.TryGetProperty("folder", out _))
                {
                    if (item.TryGetProperty("id", out var idProp))
                    {
                        count += await EnqueueChildren(library, idProp.GetString(), extFilter);
                    }
                }
                else if (item.TryGetProperty("file", out _))
                {
                    var fileName = item.TryGetProperty("name", out var n) ? n.GetString() : null;
                    if (extFilter is null
                        || (fileName?.EndsWith("." + extFilter, StringComparison.OrdinalIgnoreCase) ?? false))
                    {
                        await EnqueueFile(item, library);
                        count++;
                    }
                }
            }

            url = page.TryGetProperty("@odata.nextLink", out var next)
                ? next.GetString()
                : null;
        }

        return count;
    }

    private async Task EnqueueFile(JsonElement item, LibraryEntry library)
    {
        var message = new
        {
            driveId = library.DriveId,
            library = library.Label,
            itemId = item.GetProperty("id").GetString(),
            fileName = item.GetProperty("name").GetString(),
            webUrl = item.TryGetProperty("webUrl", out var w) ? w.GetString() : null,
            lastModified = item.TryGetProperty("lastModifiedDateTime", out var lm) ? lm.GetString() : null
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
            _logger.LogError("Graph call failed {StatusCode} for {Url}: {Error}", resp.StatusCode, url, error);
            throw new InvalidOperationException($"Graph call failed: {resp.StatusCode}");
        }

        return JsonDocument.Parse(await resp.Content.ReadAsStringAsync()).RootElement;
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

    // Delete every indexed chunk tagged with this library label (uses the Search ADMIN key).
    private async Task<int> DeleteLibraryChunks(string label)
    {
        var search = Env("SEARCH_SERVICE").TrimEnd('/');
        var index = Env("SEARCH_INDEX");
        var key = Env("SEARCH_KEY");
        var filter = "library eq '" + label.Replace("'", "''") + "'";
        int total = 0;

        for (int guard = 0; guard < 1000; guard++)
        {
            var queryBody = JsonSerializer.Serialize(new { search = "*", filter, select = "id", top = 1000 });
            using var queryReq = new HttpRequestMessage(
                HttpMethod.Post,
                $"{search}/indexes/{index}/docs/search?api-version=2024-07-01");
            queryReq.Headers.Add("api-key", key);
            queryReq.Content = new StringContent(queryBody, Encoding.UTF8, "application/json");
            using var queryResp = await _http.SendAsync(queryReq);
            if (!queryResp.IsSuccessStatusCode) break;

            var doc = JsonDocument.Parse(await queryResp.Content.ReadAsStringAsync()).RootElement;
            var ids = doc.GetProperty("value").EnumerateArray()
                .Select(e => e.GetProperty("id").GetString())
                .Where(id => !string.IsNullOrEmpty(id))
                .ToList();
            if (ids.Count == 0) break;

            var actions = ids.Select(id => new Dictionary<string, object>
            {
                ["@search.action"] = "delete",
                ["id"] = id!
            });
            var deleteBody = JsonSerializer.Serialize(new { value = actions });
            using var deleteReq = new HttpRequestMessage(
                HttpMethod.Post,
                $"{search}/indexes/{index}/docs/index?api-version=2024-07-01");
            deleteReq.Headers.Add("api-key", key);
            deleteReq.Content = new StringContent(deleteBody, Encoding.UTF8, "application/json");
            using var deleteResp = await _http.SendAsync(deleteReq);
            if (!deleteResp.IsSuccessStatusCode) break;

            total += ids.Count;
            await Task.Delay(800); // let the index settle before the next page
        }

        return total;
    }

    private static string Env(string key) =>
        Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"Missing setting: {key}");
}
