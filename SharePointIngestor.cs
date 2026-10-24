using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using UglyToad.PdfPig;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;
using System.Security.Cryptography;

public class SharePointIngestor
{
    private readonly ILogger<SharePointIngestor> _logger;
    private readonly HttpClient _http = new();

    public SharePointIngestor(ILogger<SharePointIngestor> logger)
    {
        _logger = logger;
    }

    [Function(nameof(SharePointIngestor))]
    public async Task Run([TimerTrigger("0 0 * * * *")] TimerInfo timer)
    {
        _logger.LogInformation($"SharePoint ingestion started @ {DateTime.UtcNow}");

        string token = await GetGraphToken();
        _http.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token);

        var root = await GraphGet(
            $"https://graph.microsoft.com/v1.0/drives/{Env("DRIVE_ID")}/root/children");

        var items = root.GetProperty("value").EnumerateArray().ToList();
        _logger.LogInformation($"Discovered {items.Count} items at root level.");

        foreach (var item in items)
        {
            string name = item.GetProperty("name").GetString();

            if (item.TryGetProperty("folder", out _))
            {
                _logger.LogInformation($"Entering folder: {name}");
                await ProcessFolder(item.GetProperty("id").GetString());
            }
            else
            {
                await ProcessFile(item);
            }
        }
    }


    private async Task ProcessFolder(string folderId)
    {
        var children = await GraphGet(
            $"https://graph.microsoft.com/v1.0/drives/{Env("DRIVE_ID")}/items/{folderId}/children");

        foreach (var item in children.GetProperty("value").EnumerateArray())
        {
            // Folder
            if (item.TryGetProperty("folder", out _))
            {
                if (item.TryGetProperty("id", out var folderIdProp))
                {
                    await ProcessFolder(folderIdProp.GetString());
                }
            }
            // File
            else if (item.TryGetProperty("file", out _))
            {
                await ProcessFile(item);
            }
            // Everything else (shortcuts, unknown types)
            else
            {
                _logger.LogInformation("Skipping non-file, non-folder item.");
            }
        }
    }


    private async Task ProcessFile(JsonElement file)
    {
        string itemId = file.GetProperty("id").GetString()!;
        string name = file.GetProperty("name").GetString();
        string downloadUrl = file.GetProperty("@microsoft.graph.downloadUrl").GetString();
        string? webUrl = GetOptionalString(file, "webUrl");
        DateTimeOffset? lastModified = GetOptionalDateTimeOffset(file, "lastModifiedDateTime");

        _logger.LogInformation($"Processing file: {name}");

        byte[] bytes = await _http.GetByteArrayAsync(downloadUrl);
        string text = ExtractText(name, bytes);

        if (string.IsNullOrWhiteSpace(text))
        {
            _logger.LogInformation($"No extractable text in: {name}");
            return;
        }

        var chunks = Chunk(text, 2000);
        _logger.LogInformation($"File {name} split into {chunks.Count} chunks.");

        foreach (var chunk in chunks)
        {
            float[] embedding = await CreateEmbedding(chunk);
            await PushToSearch(
                id: $"{itemId}-{HashChunk(chunk)}",
                title: name,
                url: webUrl,
                lastModified: lastModified,
                content: chunk,
                embedding: embedding);
        }
    }

    private string ExtractText(string fileName, byte[] bytes)
    {
        if (fileName.EndsWith(".txt", StringComparison.OrdinalIgnoreCase))
        {
            return Encoding.UTF8.GetString(bytes);
        }

        if (fileName.EndsWith(".pdf", StringComparison.OrdinalIgnoreCase))
        {
            var sb = new StringBuilder();
            using var pdf = PdfDocument.Open(bytes);

            foreach (var page in pdf.GetPages())
            {
                sb.AppendLine(page.Text);
            }

            return sb.ToString();
        }

        if (fileName.EndsWith(".docx", StringComparison.OrdinalIgnoreCase))
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
        {
            list.Add(text.Substring(i, Math.Min(size, text.Length - i)));
        }
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

        var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

        return doc.RootElement
            .GetProperty("data")[0]
            .GetProperty("embedding")
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
                new
                {
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
