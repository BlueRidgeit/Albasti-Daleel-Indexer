using System.Text;
using Azure.Data.Tables;

/// <summary>
/// Tracks which Azure AI Search chunk documents belong to each SharePoint item so that, when a
/// file is deleted in SharePoint, the crawler can remove its chunks from the index.
/// Backed by Azure Table Storage (the AzureWebJobsStorage account) — needs no index schema change.
/// Note: only covers files (re)indexed after this was introduced; older chunks have no registry
/// entry, so a one-time full re-index is needed for them to become deletable.
/// </summary>
public static class IndexRegistry
{
    private const string TableName = "ChunkRegistry";

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

    public static async Task RecordAsync(string itemId, string chunkHash, string searchId)
    {
        var entity = new TableEntity(Encode(itemId), chunkHash)
        {
            { "SearchId", searchId }
        };

        await GetTable().UpsertEntityAsync(entity, TableUpdateMode.Replace);
    }

    public static async Task<List<string>> GetSearchIdsAsync(string itemId)
    {
        var searchIds = new List<string>();
        var partition = Encode(itemId);

        await foreach (var entity in GetTable().QueryAsync<TableEntity>(e => e.PartitionKey == partition))
        {
            if (entity.TryGetValue("SearchId", out var value) && value is string searchId)
            {
                searchIds.Add(searchId);
            }
        }

        return searchIds;
    }

    public static async Task RemoveAsync(string itemId)
    {
        var table = GetTable();
        var partition = Encode(itemId);

        await foreach (var entity in table.QueryAsync<TableEntity>(e => e.PartitionKey == partition))
        {
            await table.DeleteEntityAsync(entity.PartitionKey, entity.RowKey);
        }
    }

    // Table keys forbid characters such as / \ # ? — hex-encode the Graph item id to stay safe.
    private static string Encode(string itemId) =>
        Convert.ToHexString(Encoding.UTF8.GetBytes(itemId));

    private static string Env(string key) =>
        Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"Missing setting: {key}");
}
