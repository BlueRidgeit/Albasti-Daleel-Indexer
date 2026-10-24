using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using System.Text;
using System.Text.Json;

public class RetryWorker
{
    private readonly ILogger<RetryWorker> _logger;
    private readonly QueueClient _retryQueue;
    private readonly QueueClient _mainQueue;

    public RetryWorker(ILogger<RetryWorker> logger)
    {
        _logger = logger;

        _retryQueue = new QueueClient(
            Environment.GetEnvironmentVariable("AzureWebJobsStorage"),
            "spo-ingestion-retry");

        _mainQueue = new QueueClient(
            Environment.GetEnvironmentVariable("AzureWebJobsStorage"),
            "spo-ingestion-queue");

        _retryQueue.CreateIfNotExists();
        _mainQueue.CreateIfNotExists();
    }

    [Function(nameof(RetryWorker))]
    public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo timer)
    {
        _logger.LogInformation("Retry worker tick");

        int processed = 0;

        // Drain in batches (32 = max per receive) so the retry queue can actually keep up,
        // instead of the old 16-messages-per-10-minutes trickle that let retries pile into millions.
        for (int batch = 0; batch < 32; batch++)
        {
            QueueMessage[] messages = await _retryQueue.ReceiveMessagesAsync(32);
            if (messages.Length == 0)
                break;

            foreach (var msg in messages)
            {
                var decoded =
                    Encoding.UTF8.GetString(Convert.FromBase64String(msg.MessageText));

                var retry = JsonSerializer.Deserialize<RetryChunkMessage>(decoded);
                if (retry == null)
                {
                    await _retryQueue.DeleteMessageAsync(msg.MessageId, msg.PopReceipt);
                    continue;
                }

                if (retry.RetryAfterUtc > DateTime.UtcNow)
                    continue;

                string retryPayloadB64 = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
                    {
                        itemId = retry.ItemId,
                        driveId = retry.DriveId,
                        library = retry.Library,
                        fileName = retry.FileName,
                        webUrl = retry.WebUrl,
                        lastModified = retry.LastModified,
                        chunk = retry.Chunk,
                        isRetry = true
                    }))
                );

                await _mainQueue.SendMessageAsync(retryPayloadB64);
                await _retryQueue.DeleteMessageAsync(msg.MessageId, msg.PopReceipt);
                processed++;
            }
        }

        _logger.LogInformation($"Retry worker processed {processed} messages");
    }
}
