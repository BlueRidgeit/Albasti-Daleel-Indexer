using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Data.Tables;
using Azure.Storage.Queues;
using System.Text;
using System.Text.Json;

/// <summary>
/// Read-only indexing-health snapshot for the admin "Indexing Health" panel in the web app.
///
///   GET /api/status  ->  { generatedAt, queue:{ingestion,retry}, libraries:[ { driveId, label,
///                          enabled, indexedCount, lastSyncedAt } ] }
///
/// - indexedCount  : per-library chunk count from the search index facet.
/// - lastSyncedAt  : the DeltaState row Timestamp — updated every crawl tick (~10 min) when the
///                   crawler finishes that library's delta. A value older than ~1h means the
///                   crawler is stuck/down for that library (exactly the failure we hit when the
///                   Judgement delta cursor froze).
/// - queue         : approximate depth of the ingestion + retry queues (work still pending).
/// </summary>
public class Status
{
    private readonly ILogger<Status> _logger;
    private readonly HttpClient _http = new();

    public Status(ILogger<Status> logger)
    {
        _logger = logger;
    }

    [Function("Status")]
    public async Task<IResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = "status")] HttpRequest req)
    {
        var storage = Env("AzureWebJobsStorage");

        // Per-library indexed chunk counts from the search index facet.
        var counts = await GetIndexedCounts();

        // Libraries + their last-synced cursor timestamp (DeltaState table, PK "Drive").
        var libraries = await LibraryStore.GetAllAsync();
        var deltaTable = new TableClient(storage, "DeltaState");
        await deltaTable.CreateIfNotExistsAsync();

        var libStatus = new List<object>();
        foreach (var lib in libraries)
        {
            DateTimeOffset? lastSynced = null;
            try
            {
                var entity = await deltaTable.GetEntityAsync<TableEntity>("Drive", lib.DriveId);
                lastSynced = entity.Value.Timestamp;
            }
            catch
            {
                // No delta row yet (never crawled) — leave lastSyncedAt null.
            }

            libStatus.Add(new
            {
                driveId = lib.DriveId,
                label = lib.Label,
                enabled = lib.Enabled,
                indexedCount = counts.TryGetValue(lib.Label, out var c) ? c : 0L,
                lastSyncedAt = lastSynced?.UtcDateTime.ToString("o"),
            });
        }

        long ingestion = await QueueDepth(storage, "spo-ingestion-queue");
        long retry = await QueueDepth(storage, "spo-ingestion-retry");

        return Results.Ok(new
        {
            generatedAt = DateTime.UtcNow.ToString("o"),
            queue = new { ingestion, retry },
            libraries = libStatus,
        });
    }

    private static async Task<long> QueueDepth(string storage, string name)
    {
        try
        {
            var queue = new QueueClient(storage, name);
            if (!await queue.ExistsAsync())
            {
                return 0;
            }
            var props = await queue.GetPropertiesAsync();
            return props.Value.ApproximateMessagesCount;
        }
        catch
        {
            return -1; // unknown / unreachable
        }
    }

    private async Task<Dictionary<string, long>> GetIndexedCounts()
    {
        var counts = new Dictionary<string, long>();
        try
        {
            var search = Env("SEARCH_SERVICE").TrimEnd('/');
            var index = Env("SEARCH_INDEX");
            var key = Env("SEARCH_KEY");

            var body = JsonSerializer.Serialize(new
            {
                search = "*",
                top = 0,
                count = true,
                facets = new[] { "library,count:100" },
            });

            using var reqMsg = new HttpRequestMessage(
                HttpMethod.Post,
                $"{search}/indexes/{index}/docs/search?api-version=2024-07-01");
            reqMsg.Headers.Add("api-key", key);
            reqMsg.Content = new StringContent(body, Encoding.UTF8, "application/json");

            using var resp = await _http.SendAsync(reqMsg);
            if (!resp.IsSuccessStatusCode)
            {
                return counts;
            }

            var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync()).RootElement;
            if (doc.TryGetProperty("@search.facets", out var facets)
                && facets.TryGetProperty("library", out var lib))
            {
                foreach (var facet in lib.EnumerateArray())
                {
                    var value = facet.TryGetProperty("value", out var v) ? v.GetString() : null;
                    var count = facet.TryGetProperty("count", out var cc) ? cc.GetInt64() : 0L;
                    if (!string.IsNullOrEmpty(value))
                    {
                        counts[value!] = count;
                    }
                }
            }
        }
        catch
        {
            // Search unreachable — return whatever we have (counts stay 0 in the UI).
        }
        return counts;
    }

    private static string Env(string key) =>
        Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"Missing setting: {key}");
}
