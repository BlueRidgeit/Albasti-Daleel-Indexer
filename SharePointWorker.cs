using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using UglyToad.PdfPig;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;
using Azure.Storage.Queues;
using System.Security.Cryptography;


public class SharePointWorker
{
    private readonly ILogger<SharePointWorker> _logger;
    private readonly HttpClient _http = new();
    private static readonly SemaphoreSlim EmbeddingGate = new(1, 1);

    private readonly QueueClient _retryQueue;

    public SharePointWorker(ILogger<SharePointWorker> logger)
    {

        _logger = logger;

        _retryQueue = new QueueClient(
            Env("AzureWebJobsStorage"),
            "spo-ingestion-retry");

        _retryQueue.CreateIfNotExists();

    }

    [Function(nameof(SharePointWorker))]
    public async Task Run(
        [QueueTrigger("spo-ingestion-queue", Connection = "AzureWebJobsStorage")]
        string message)
    {
        var payload = JsonDocument.Parse(message).RootElement;

        // Declare ONCE (no redeclaration later)
        string itemId = payload.GetProperty("itemId").GetString()!;
        string fileName = payload.GetProperty("fileName").GetString()!
                                .Trim()
                                .Replace("\0", "");
        string? webUrl = GetOptionalString(payload, "webUrl");
        DateTimeOffset? lastModified = GetOptionalDateTimeOffset(payload, "lastModified");

        // =========================================================
        // ✅ FAST‑PATH: retrying a single failed chunk
        // =========================================================
        if (payload.TryGetProperty("isRetry", out var retryFlag) &&
            retryFlag.GetBoolean())
        {
            string chunk = payload.GetProperty("chunk").GetString()!;

            _logger.LogInformation($"Retrying chunk for {fileName}");

            try
            {
                if (string.IsNullOrWhiteSpace(webUrl) || lastModified == null)
                {
                    var metadata = await GetFileMetadata(itemId);
                    webUrl ??= metadata.WebUrl;
                    lastModified ??= metadata.LastModified;
                }

                await EmbeddingGate.WaitAsync();
                try
                {
                    var embedding = await CreateEmbedding(chunk);
                    var chunkHash = HashChunk(chunk);

                    await PushToSearch(
                        id: $"{itemId}-{chunkHash}",
                        title: fileName,
                        url: webUrl,
                        lastModified: lastModified,
                        content: chunk,
                        embedding: embedding
                    );
                }
                finally
                {
                    EmbeddingGate.Release();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Final retry failed for {fileName}");
            }

            return;
        }

        _logger.LogInformation($"Worker processing {fileName}");

        // =========================================================
        // ✅ Supported file types
        // =========================================================
        if (!(
            fileName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith(".docx", StringComparison.OrdinalIgnoreCase) ||
            fileName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase)
        ))
        {
            _logger.LogInformation($"Skipping unsupported file type: {fileName}");
            return;
        }

        // =========================================================
        // ✅ Fetch file from SharePoint
        // =========================================================
        string token = await GetGraphToken();
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);

        var file = await GraphGet(
            $"https://graph.microsoft.com/v1.0/drives/{Env("DRIVE_ID")}/items/{itemId}");

        webUrl ??= GetOptionalString(file, "webUrl");
        lastModified ??= GetOptionalDateTimeOffset(file, "lastModifiedDateTime");

        if (!file.TryGetProperty("@microsoft.graph.downloadUrl", out var urlProp))
        {
            _logger.LogInformation($"No download URL for {fileName}");
            return;
        }

        byte[] bytes = null!;
        string downloadUrl = urlProp.GetString()!;

        await Task.Delay(500);

        for (int attempt = 1; attempt <= 5; attempt++)
        {
            try
            {
                bytes = await _http.GetByteArrayAsync(downloadUrl);
                break;
            }
            catch (HttpRequestException ex) when (
                ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests ||
                ex.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable ||
                ex.StatusCode == System.Net.HttpStatusCode.GatewayTimeout
            )
            {
                await Task.Delay(2000 * attempt);
            }
        }

        if (bytes == null)
        {
            _logger.LogError($"Failed to download {fileName}");
            return;
        }

        // =========================================================
        // ✅ Extract + chunk
        // =========================================================
        var text = ExtractText(fileName, bytes);

        if (string.IsNullOrWhiteSpace(text) || text.Length < 50)
        {
            _logger.LogInformation($"Skipping empty document: {fileName}");
            return;
        }

        var chunks = Chunk(text, 2000);

        // =========================================================
        // ✅ Index chunks
        // =========================================================
        foreach (var chunk in chunks)
        {
            try
            {
                await EmbeddingGate.WaitAsync();
                try
                {
                    var embedding = await CreateEmbedding(chunk);
                    var chunkHash = HashChunk(chunk);

                    await PushToSearch(
                        id: $"{itemId}-{chunkHash}",
                        title: fileName,
                        url: webUrl,
                        lastModified: lastModified,
                        content: chunk,
                        embedding: embedding
                    );
                }
                finally
                {
                    EmbeddingGate.Release();
                }
            }
            catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
            {
                _logger.LogWarning($"Embedding throttled, scheduling retry for {fileName}");

                var retryMsg = new RetryChunkMessage
                {
                    ItemId = itemId,
                    FileName = fileName,
                    WebUrl = webUrl,
                    LastModified = lastModified,
                    Chunk = chunk,
                    RetryAfterUtc = DateTime.UtcNow.AddMinutes(10)
                };

                string retryPayloadB64 = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(JsonSerializer.Serialize(retryMsg)));

                await _retryQueue.SendMessageAsync(retryPayloadB64);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, $"Dropping invalid chunk for {fileName}");
            }
        }
    }

    private string ExtractText(string name, byte[] bytes)
    {
        if (name.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
            return Encoding.UTF8.GetString(bytes);

        if (name.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
        {
            var sb = new StringBuilder();
            using var pdf = PdfDocument.Open(bytes);
            foreach (var page in pdf.GetPages())
                sb.AppendLine(page.Text);
            return sb.ToString();
        }

        if (name.EndsWith(".docx", StringComparison.OrdinalIgnoreCase))
        {
            using var ms = new MemoryStream(bytes);
            using var doc = WordprocessingDocument.Open(ms, false);

            return string.Join(
                "\n",
                doc.MainDocumentPart!
                    .Document!
                    .Body!
                    .Descendants<Text>()
                    .Select(t => t.Text)
            );
        }

        return string.Empty;
    }

    private List<string> Chunk(string text, int size)
    {
        var list = new List<string>();
        for (int i = 0; i < text.Length; i += size)
            list.Add(text.Substring(i, Math.Min(size, text.Length - i)));
        return list;
    }

    private async Task<float[]> CreateEmbedding(string input)
    {
        _http.DefaultRequestHeaders.Clear();
        _http.DefaultRequestHeaders.Add("api-key", Env("EMBEDDING_KEY"));

        var body = new { input };

        var response = await _http.PostAsync(
            $"{Env("EMBEDDING_ENDPOINT")}/openai/deployments/{Env("EMBEDDING_MODEL")}/embeddings?api-version=2024-02-15-preview",
            new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"));

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync();
            _logger.LogWarning($"Embedding API returned {response.StatusCode}: {error}");
            throw new InvalidOperationException("Embedding call failed");
        }

        var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

        if (!doc.RootElement.TryGetProperty("data", out var dataArray) ||
            dataArray.GetArrayLength() == 0 ||
            !dataArray[0].TryGetProperty("embedding", out var embeddingElement))
        {
            _logger.LogWarning("Embedding response missing expected fields");
            throw new InvalidOperationException("Invalid embedding response");
        }

        return embeddingElement
            .EnumerateArray()
            .Select(v => v.GetSingle())
            .ToArray();
    }

    private async Task PushToSearch(
        string id,
        string title,
        string? url,
        DateTimeOffset? lastModified,
        string content,
        float[] embedding)
    {
        var body = new
        {
            value = new[]
            {
                new {
                    id,
                    title,
                    url,
                    lastModified,
                    content,
                    embedding
                }
            }
        };

        _http.DefaultRequestHeaders.Clear();
        _http.DefaultRequestHeaders.Add("api-key", Env("SEARCH_KEY"));

        await _http.PostAsync(
            $"{Env("SEARCH_SERVICE")}/indexes/{Env("SEARCH_INDEX")}/docs/index?api-version=2025-11-01-preview",
            new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json"));
    }

    private async Task<JsonElement> GraphGet(string url)
    {
        var resp = await _http.GetAsync(url);
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

    private async Task<FileMetadata> GetFileMetadata(string itemId)
    {
        string token = await GetGraphToken();
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);

        var file = await GraphGet(
            $"https://graph.microsoft.com/v1.0/drives/{Env("DRIVE_ID")}/items/{itemId}");

        return new FileMetadata(
            GetOptionalString(file, "webUrl"),
            GetOptionalDateTimeOffset(file, "lastModifiedDateTime"));
    }

    private static string Env(string key) =>
        Environment.GetEnvironmentVariable(key)
        ?? throw new InvalidOperationException($"Missing setting: {key}");

    private static string? GetOptionalString(JsonElement element, string propertyName)
    {
        if (!element.TryGetProperty(propertyName, out var property) ||
            property.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        return property.GetString();
    }

    private static DateTimeOffset? GetOptionalDateTimeOffset(JsonElement element, string propertyName)
    {
        var raw = GetOptionalString(element, propertyName);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        return DateTimeOffset.TryParse(raw, out var parsed) ? parsed : null;
    }

    private static string HashChunk(string text)
    {
        using var sha = SHA256.Create();
        var bytes = Encoding.UTF8.GetBytes(text);
        var hash = sha.ComputeHash(bytes);

        return Convert.ToHexString(hash);
    }
}


public class RetryChunkMessage
{
    public string ItemId { get; set; } = default!;
    public string FileName { get; set; } = default!;
    public string? WebUrl { get; set; }
    public DateTimeOffset? LastModified { get; set; }
    public string Chunk { get; set; } = default!;
    public DateTime RetryAfterUtc { get; set; }
}

public record FileMetadata(string? WebUrl, DateTimeOffset? LastModified);


