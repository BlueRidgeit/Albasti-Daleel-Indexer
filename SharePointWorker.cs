using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using System.Net;
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

    // One shared HttpClient; per-call headers are set on each HttpRequestMessage instead of
    // mutating DefaultRequestHeaders (which is not safe to share across calls).
    private static readonly HttpClient Http = new();
    // Allow several concurrent embedding calls (the 350K TPM quota supports it). 429s are
    // already rerouted to the retry queue, so over-driving is self-correcting. Was new(1,1),
    // which serialized embeds to one-at-a-time and capped throughput far below quota.
    private static readonly SemaphoreSlim EmbeddingGate = new(16, 16);

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

        string itemId = payload.GetProperty("itemId").GetString()!;
        string fileName = payload.GetProperty("fileName").GetString()!
                                .Trim()
                                .Replace("\0", "");
        string? webUrl = GetOptionalString(payload, "webUrl");
        DateTimeOffset? lastModified = GetOptionalDateTimeOffset(payload, "lastModified");

        // Per-library: the drive + library label travel on the message. Fall back to the legacy
        // single DRIVE_ID for any older messages still on the queue.
        string driveId = GetOptionalString(payload, "driveId") ?? Env("DRIVE_ID");
        string? library = GetOptionalString(payload, "library");
        // Carried on retry messages; computed from the file's parentReference on the main path.
        string? folderUrl = GetOptionalString(payload, "folderUrl");

        // =========================================================
        // Fast-path: retrying a single throttled chunk
        // =========================================================
        if (payload.TryGetProperty("isRetry", out var retryFlag) && retryFlag.GetBoolean())
        {
            string chunk = payload.GetProperty("chunk").GetString()!;
            _logger.LogInformation($"Retrying chunk for {fileName}");

            try
            {
                if (string.IsNullOrWhiteSpace(webUrl) || lastModified == null)
                {
                    var metadata = await GetFileMetadata(itemId, driveId);
                    webUrl ??= metadata.WebUrl;
                    lastModified ??= metadata.LastModified;
                }

                await IndexChunkAsync(itemId, driveId, library, fileName, webUrl, folderUrl, lastModified, chunk);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Final retry failed for {fileName}");
            }

            return;
        }

        _logger.LogInformation($"Worker processing {fileName} [{library ?? "?"}]");

        // Office leaves "~$name.docx" lock/owner files (~162 bytes, not real documents).
        // They aren't valid Office packages, so extraction throws — skip them outright.
        if (fileName.StartsWith("~$"))
        {
            _logger.LogInformation($"Skipping Office temp/lock file: {fileName}");
            return;
        }

        // =========================================================
        // Supported file types
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
        // Fetch file metadata + download URL from SharePoint
        // =========================================================
        string token = await GetGraphToken();

        var file = await GraphGet(
            $"https://graph.microsoft.com/v1.0/drives/{driveId}/items/{itemId}", token);

        webUrl ??= GetOptionalString(file, "webUrl");
        lastModified ??= GetOptionalDateTimeOffset(file, "lastModifiedDateTime");
        folderUrl ??= await ComputeFolderUrlAsync(file, driveId, token);

        if (!file.TryGetProperty("@microsoft.graph.downloadUrl", out var urlProp))
        {
            _logger.LogWarning($"No download URL for {fileName}");
            return;
        }

        string downloadUrl = urlProp.GetString()!;
        byte[] bytes = await DownloadWithRetry(downloadUrl, fileName);

        // =========================================================
        // Extract + chunk
        // =========================================================
        string text;
        try
        {
            text = await ExtractTextAsync(fileName, bytes);
        }
        catch (Exception ex)
        {
            // A corrupt / password-protected / malformed file should be skipped, NOT poisoned
            // (a thrown exception here would dead-letter the message and stall the backlog).
            _logger.LogWarning($"Text extraction failed for {fileName}; skipping. {ex.Message}");
            return;
        }

        if (string.IsNullOrWhiteSpace(text) || text.Length < 50)
        {
            _logger.LogInformation($"Skipping empty document: {fileName}");
            return;
        }

        var chunks = Chunk(text, 4000, 300);

        // =========================================================
        // Index chunks
        // =========================================================
        foreach (var chunk in chunks)
        {
            try
            {
                await IndexChunkAsync(itemId, driveId, library, fileName, webUrl, folderUrl, lastModified, chunk);
            }
            catch (HttpRequestException ex) when (ex.StatusCode == HttpStatusCode.TooManyRequests)
            {
                _logger.LogWarning($"Embedding throttled, scheduling retry for {fileName}");

                var retryMsg = new RetryChunkMessage
                {
                    ItemId = itemId,
                    DriveId = driveId,
                    Library = library,
                    FileName = fileName,
                    WebUrl = webUrl,
                    FolderUrl = folderUrl,
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
                // Surface the failure (don't swallow). Skip just this chunk so one bad chunk
                // doesn't strand the rest of the document.
                _logger.LogError(ex, $"Failed to index a chunk of {fileName}; skipping that chunk.");
            }
        }
    }

    private async Task IndexChunkAsync(
        string itemId, string driveId, string? library,
        string fileName, string? webUrl, string? folderUrl, DateTimeOffset? lastModified, string chunk)
    {
        await EmbeddingGate.WaitAsync();
        try
        {
            var embedding = await CreateEmbedding(chunk);
            var chunkHash = HashChunk(chunk);
            var searchId = $"{itemId}-{chunkHash}";

            await PushToSearch(
                id: searchId,
                itemId: itemId,
                library: library,
                driveId: driveId,
                title: fileName,
                url: webUrl,
                folderUrl: folderUrl,
                fileType: FileTypeFromName(fileName),
                lastModified: lastModified,
                content: chunk,
                embedding: embedding);

            // Track the chunk so the crawler can delete it if the file is later removed.
            await IndexRegistry.RecordAsync(itemId, chunkHash, searchId);
        }
        finally
        {
            EmbeddingGate.Release();
        }
    }

    private async Task<byte[]> DownloadWithRetry(string downloadUrl, string fileName)
    {
        for (int attempt = 1; attempt <= 5; attempt++)
        {
            try
            {
                return await Http.GetByteArrayAsync(downloadUrl);
            }
            catch (HttpRequestException ex) when (
                attempt < 5 &&
                ex.StatusCode is HttpStatusCode.TooManyRequests
                              or HttpStatusCode.ServiceUnavailable
                              or HttpStatusCode.GatewayTimeout)
            {
                await Task.Delay(2000 * attempt);
            }
        }

        // Surface the failure so the queue retries (then dead-letters) instead of silently dropping the file.
        throw new InvalidOperationException($"Failed to download {fileName} from SharePoint after retries.");
    }

    private async Task<string> ExtractTextAsync(string name, byte[] bytes)
    {
        if (name.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
            return Encoding.UTF8.GetString(bytes);

        if (name.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
        {
            // Prefer Azure AI Document Intelligence (OCR "Read"): it returns clean, logical-order
            // Unicode for Arabic and also reads scanned/image PDFs. PdfPig (which mangles Arabic
            // into reversed presentation-form glyphs) is kept only as a fallback for the rare case
            // Document Intelligence is unavailable or returns nothing.
            try
            {
                var ocr = await ExtractPdfWithDocIntelAsync(bytes);
                if (!string.IsNullOrWhiteSpace(ocr))
                    return ocr;
                _logger.LogWarning($"Document Intelligence returned empty text for {name}; falling back to PdfPig.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning($"Document Intelligence failed for {name}: {ex.Message}. Falling back to PdfPig.");
            }

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

    // Azure AI Document Intelligence "prebuilt-read" OCR: POST the bytes, then poll the async
    // operation until it succeeds and return the concatenated reading-order content.
    private async Task<string> ExtractPdfWithDocIntelAsync(byte[] bytes)
    {
        string endpoint = Env("DOCINTEL_ENDPOINT").TrimEnd('/');
        string key = Env("DOCINTEL_KEY");
        string analyzeUrl =
            $"{endpoint}/documentintelligence/documentModels/prebuilt-read:analyze?api-version=2024-11-30";

        // Start the analysis; retry on throttling (429/503) so a re-index storm doesn't fall back
        // to the garbled PdfPig path.
        string? opLoc = null;
        for (int attempt = 0; attempt < 6 && opLoc is null; attempt++)
        {
            using var startReq = new HttpRequestMessage(HttpMethod.Post, analyzeUrl);
            startReq.Headers.Add("Ocp-Apim-Subscription-Key", key);
            startReq.Content = new ByteArrayContent(bytes);
            startReq.Content.Headers.ContentType = new MediaTypeHeaderValue("application/pdf");

            using var startResp = await Http.SendAsync(startReq);

            if (startResp.StatusCode == HttpStatusCode.TooManyRequests
                || startResp.StatusCode == HttpStatusCode.ServiceUnavailable)
            {
                int wait = startResp.Headers.RetryAfter?.Delta?.Seconds ?? (5 * (attempt + 1));
                await Task.Delay(TimeSpan.FromSeconds(Math.Max(2, wait)));
                continue;
            }

            if (!startResp.IsSuccessStatusCode)
            {
                var err = await startResp.Content.ReadAsStringAsync();
                throw new InvalidOperationException($"analyze start failed {(int)startResp.StatusCode}: {err}");
            }

            opLoc = startResp.Headers.TryGetValues("Operation-Location", out var locs)
                ? locs.First()
                : throw new InvalidOperationException("Document Intelligence missing Operation-Location header");
        }

        if (opLoc is null)
            throw new InvalidOperationException("Document Intelligence throttled (429) after retries");

        // Poll up to ~3 minutes (Read is usually seconds; large scanned PDFs take longer).
        for (int i = 0; i < 90; i++)
        {
            await Task.Delay(2000);

            using var pollReq = new HttpRequestMessage(HttpMethod.Get, opLoc);
            pollReq.Headers.Add("Ocp-Apim-Subscription-Key", key);
            using var pollResp = await Http.SendAsync(pollReq);
            pollResp.EnsureSuccessStatusCode();

            using var doc = JsonDocument.Parse(await pollResp.Content.ReadAsStringAsync());
            string status = doc.RootElement.GetProperty("status").GetString() ?? "";

            if (status == "succeeded")
            {
                return doc.RootElement.TryGetProperty("analyzeResult", out var ar)
                       && ar.TryGetProperty("content", out var content)
                    ? content.GetString() ?? string.Empty
                    : string.Empty;
            }

            if (status == "failed")
                throw new InvalidOperationException("Document Intelligence reported status=failed");
        }

        throw new TimeoutException("Document Intelligence analysis timed out");
    }

    // Word-boundary chunking with overlap so retrieval keeps context across chunk edges
    // (instead of slicing every `size` characters mid-word with no overlap).
    private static List<string> Chunk(string text, int size, int overlap = 200)
    {
        var chunks = new List<string>();
        if (string.IsNullOrWhiteSpace(text)) return chunks;

        int pos = 0;
        int n = text.Length;

        while (pos < n)
        {
            int end = Math.Min(pos + size, n);

            if (end < n)
            {
                // Prefer cutting at the last whitespace in the tail of the window to avoid splitting a word.
                int window = Math.Min(200, end - pos - 1);
                if (window > 0)
                {
                    int sp = text.LastIndexOf(' ', end - 1, window);
                    if (sp > pos) end = sp;
                }
            }

            var piece = text.Substring(pos, end - pos).Trim();
            if (piece.Length > 0) chunks.Add(piece);

            if (end >= n) break;

            int next = end - overlap;
            pos = next > pos ? next : end; // guarantee forward progress
        }

        return chunks;
    }

    private async Task<float[]> CreateEmbedding(string input)
    {
        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{Env("EMBEDDING_ENDPOINT")}/openai/deployments/{Env("EMBEDDING_MODEL")}/embeddings?api-version=2024-02-15-preview");
        request.Headers.Add("api-key", Env("EMBEDDING_KEY"));
        request.Content = new StringContent(
            JsonSerializer.Serialize(new { input, dimensions = 768 }), Encoding.UTF8, "application/json");

        using var response = await Http.SendAsync(request);

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync();
            _logger.LogWarning($"Embedding API returned {response.StatusCode}: {error}");
            // Preserve the status code so the 429 retry path in Run still works.
            throw new HttpRequestException(
                $"Embedding call failed: {response.StatusCode}", null, response.StatusCode);
        }

        var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

        if (!doc.RootElement.TryGetProperty("data", out var dataArray) ||
            dataArray.GetArrayLength() == 0 ||
            !dataArray[0].TryGetProperty("embedding", out var embeddingElement))
        {
            throw new InvalidOperationException("Invalid embedding response");
        }

        return embeddingElement
            .EnumerateArray()
            .Select(v => v.GetSingle())
            .ToArray();
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, string> DriveRootUrlCache = new();

    private static string FileTypeFromName(string fileName)
    {
        if (fileName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase)) return "pdf";
        if (fileName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase)) return "text";
        if (fileName.EndsWith(".docx", StringComparison.OrdinalIgnoreCase)
            || fileName.EndsWith(".doc", StringComparison.OrdinalIgnoreCase)) return "word";
        return "other";
    }

    // Build a clickable SharePoint folder URL from the file's parentReference.path by mapping
    // the part after "root:" onto the drive's root webUrl (cached per drive).
    private async Task<string?> ComputeFolderUrlAsync(JsonElement file, string driveId, string token)
    {
        if (!file.TryGetProperty("parentReference", out var pr)
            || !pr.TryGetProperty("path", out var pathProp))
            return null;

        var path = pathProp.GetString();
        if (string.IsNullOrEmpty(path)) return null;

        var marker = path.IndexOf("root:", StringComparison.Ordinal);
        var rel = marker >= 0 ? path[(marker + 5)..] : "";

        if (!DriveRootUrlCache.TryGetValue(driveId, out var rootUrl))
        {
            var root = await GraphGet(
                $"https://graph.microsoft.com/v1.0/drives/{driveId}/root?$select=webUrl", token);
            rootUrl = GetOptionalString(root, "webUrl") ?? "";
            DriveRootUrlCache[driveId] = rootUrl;
        }
        if (string.IsNullOrEmpty(rootUrl)) return null;

        var encoded = string.Join("/",
            rel.Split('/', StringSplitOptions.RemoveEmptyEntries).Select(Uri.EscapeDataString));
        return string.IsNullOrEmpty(encoded) ? rootUrl : $"{rootUrl}/{encoded}";
    }

    private async Task PushToSearch(
        string id,
        string itemId,
        string? library,
        string driveId,
        string title,
        string? url,
        string? folderUrl,
        string? fileType,
        DateTimeOffset? lastModified,
        string content,
        float[] embedding)
    {
        var body = new
        {
            value = new[]
            {
                new { id, itemId, library, driveId, title, url, folderUrl, fileType, lastModified, content, contentAr = content, embedding }
            }
        };

        using var request = new HttpRequestMessage(
            HttpMethod.Post,
            $"{Env("SEARCH_SERVICE")}/indexes/{Env("SEARCH_INDEX")}/docs/index?api-version=2025-11-01-preview");
        request.Headers.Add("api-key", Env("SEARCH_KEY"));
        request.Content = new StringContent(
            JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");

        using var response = await Http.SendAsync(request);

        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync();
            throw new HttpRequestException(
                $"Search index push failed: {response.StatusCode} {error}", null, response.StatusCode);
        }
    }

    private async Task<JsonElement> GraphGet(string url, string token)
    {
        // Retry on throttling / transient 5xx so a Graph rate-limit during bulk crawling
        // doesn't dead-letter the file (this call had NO retry, which poisoned files at scale).
        for (int attempt = 1; ; attempt++)
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            using var response = await Http.SendAsync(request);

            if (response.IsSuccessStatusCode)
                return JsonDocument.Parse(await response.Content.ReadAsStringAsync()).RootElement;

            bool transient = response.StatusCode is HttpStatusCode.TooManyRequests
                or HttpStatusCode.ServiceUnavailable
                or HttpStatusCode.GatewayTimeout
                or HttpStatusCode.BadGateway;

            if (transient && attempt <= 6)
            {
                int wait = response.Headers.RetryAfter?.Delta?.Seconds ?? (2 * attempt);
                await Task.Delay(TimeSpan.FromSeconds(Math.Max(2, wait)));
                continue;
            }

            var error = await response.Content.ReadAsStringAsync();
            throw new HttpRequestException(
                $"Graph GET failed: {response.StatusCode} {error}", null, response.StatusCode);
        }
    }

    // Build the confidential-client app ONCE and reuse it — MSAL caches the app-only token
    // internally and only refreshes near expiry. (Previously a new app was built + a token
    // acquired on every file, adding token-endpoint load and latency at scale.)
    private static IConfidentialClientApplication? _msalApp;
    private static readonly SemaphoreSlim _msalLock = new(1, 1);

    private async Task<string> GetGraphToken()
    {
        if (_msalApp is null)
        {
            await _msalLock.WaitAsync();
            try
            {
                _msalApp ??= ConfidentialClientApplicationBuilder
                    .Create(Env("CLIENT_ID"))
                    .WithClientSecret(Env("CLIENT_SECRET"))
                    .WithAuthority($"https://login.microsoftonline.com/{Env("TENANT_ID")}")
                    .Build();
            }
            finally
            {
                _msalLock.Release();
            }
        }

        var result = await _msalApp
            .AcquireTokenForClient(new[] { "https://graph.microsoft.com/.default" })
            .ExecuteAsync();

        return result.AccessToken;
    }

    private async Task<FileMetadata> GetFileMetadata(string itemId, string driveId)
    {
        string token = await GetGraphToken();

        var file = await GraphGet(
            $"https://graph.microsoft.com/v1.0/drives/{driveId}/items/{itemId}", token);

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
    public string DriveId { get; set; } = default!;
    public string? Library { get; set; }
    public string FileName { get; set; } = default!;
    public string? WebUrl { get; set; }
    public string? FolderUrl { get; set; }
    public DateTimeOffset? LastModified { get; set; }
    public string Chunk { get; set; } = default!;
    public DateTime RetryAfterUtc { get; set; }
}

public record FileMetadata(string? WebUrl, DateTimeOffset? LastModified);
