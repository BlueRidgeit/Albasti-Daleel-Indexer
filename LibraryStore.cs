using System.Text.Json;
using Azure.Data.Tables;

public record LibraryEntry(string DriveId, string Label, bool Enabled);

/// <summary>
/// Source of truth for which SharePoint document libraries the indexer crawls.
/// Backed by Azure Table Storage ("Libraries"). The admin "Library Management" feature and the
/// INDEX_LIBRARIES seed env populate it. When the table is empty it seeds from INDEX_LIBRARIES
/// (a JSON array of {driveId,label}) or, failing that, the legacy single DRIVE_ID as "Docs".
/// </summary>
public static class LibraryStore
{
    private const string TableName = "Libraries";
    private const string Partition = "lib";

    private static TableClient? _table;
    private static readonly object Sync = new();

    private static TableClient GetTable()
    {
        if (_table is null)
        {
            lock (Sync)
            {
                if (_table is null)
                {
                    var client = new TableClient(Env("AzureWebJobsStorage"), TableName);
                    client.CreateIfNotExists();
                    _table = client;
                }
            }
        }

        return _table;
    }

    public static async Task<List<LibraryEntry>> GetAllAsync()
    {
        var list = new List<LibraryEntry>();
        await foreach (var e in GetTable().QueryAsync<TableEntity>(x => x.PartitionKey == Partition))
        {
            list.Add(ToEntry(e));
        }
        return list;
    }

    public static async Task<List<LibraryEntry>> GetEnabledAsync()
    {
        var enabled = (await GetAllAsync()).Where(l => l.Enabled).ToList();
        if (enabled.Count == 0)
        {
            enabled = await SeedAsync();
        }
        return enabled;
    }

    public static async Task<LibraryEntry?> GetAsync(string driveId)
    {
        try
        {
            var resp = await GetTable().GetEntityAsync<TableEntity>(Partition, driveId);
            return ToEntry(resp.Value);
        }
        catch
        {
            return null;
        }
    }

    public static async Task UpsertAsync(string driveId, string label, bool enabled)
    {
        var entity = new TableEntity(Partition, driveId)
        {
            { "Label", label },
            { "Enabled", enabled }
        };

        await GetTable().UpsertEntityAsync(entity, TableUpdateMode.Merge);
    }

    // Remove a library from the crawl list entirely (used by the admin "Remove" action).
    public static async Task DeleteAsync(string driveId)
    {
        try
        {
            await GetTable().DeleteEntityAsync(Partition, driveId);
        }
        catch
        {
            // Already absent — nothing to do.
        }
    }

    private static LibraryEntry ToEntry(TableEntity e)
    {
        var label = e.TryGetValue("Label", out var l) && l is string ls ? ls : e.RowKey;
        var enabled = e.TryGetValue("Enabled", out var en) && en is bool b && b;
        return new LibraryEntry(e.RowKey, label, enabled);
    }

    private static async Task<List<LibraryEntry>> SeedAsync()
    {
        var seed = new List<LibraryEntry>();

        var json = Environment.GetEnvironmentVariable("INDEX_LIBRARIES");
        if (!string.IsNullOrWhiteSpace(json))
        {
            try
            {
                var items = JsonSerializer.Deserialize<List<SeedItem>>(json) ?? new();
                foreach (var s in items)
                {
                    if (!string.IsNullOrWhiteSpace(s.driveId))
                    {
                        var label = string.IsNullOrWhiteSpace(s.label) ? s.driveId! : s.label!;
                        seed.Add(new LibraryEntry(s.driveId!, label, true));
                    }
                }
            }
            catch
            {
                // Malformed seed JSON — fall through to the legacy single-drive seed below.
            }
        }

        if (seed.Count == 0)
        {
            var legacy = Environment.GetEnvironmentVariable("DRIVE_ID");
            if (!string.IsNullOrWhiteSpace(legacy))
                seed.Add(new LibraryEntry(legacy!, "Docs", true));
        }

        foreach (var entry in seed)
            await UpsertAsync(entry.DriveId, entry.Label, true);

        return seed;
    }

    private sealed class SeedItem
    {
        public string? driveId { get; set; }
        public string? label { get; set; }
    }

    private static string Env(string key) =>
        Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"Missing setting: {key}");
}
