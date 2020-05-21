using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;

namespace EventHubApp
{
    public class Program
    {
        private static int NUM_EVENTS = 10;

        static async Task Main(string[] args)
        {
            var eventHubConnectionString = @"Endpoint=sb://didieventhubns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tsaMBLbPzSp7fbfoRMYymTkA1E8JVKgFTHeJ5Q0KlcM=";
            var eventHubName = "didieventhub";
            var blobStorageConnectionString = @"DefaultEndpointsProtocol=https;AccountName=piransteststorage;AccountKey=AOm/Slm6+KBnoRfCUwn9TVq+7NtnRE3CyE5/8kwPgDRh1ndMHp7JDuWuSKgNS9s7GvGY5CUSPJs8mNNftPLVXQ==;EndpointSuffix=core.windows.net";
            var blobContainerName = @"azure-event-hub";

            await SendEvents(eventHubConnectionString, eventHubName);
            await Task.Delay(TimeSpan.FromMinutes(10));

            var eventHubMessageProccessor = await RegisterEventReciever(eventHubConnectionString, eventHubName, blobStorageConnectionString, blobContainerName);

            Console.WriteLine("Closing event-hub client and exiting now!");
            await eventHubMessageProccessor.StopProcessingAsync();
        }

        private static async Task SendEvents(string eventHubConnectionString, string eventHubName)
        {
            await using (var producerClient = new EventHubProducerClient(eventHubConnectionString, eventHubName))
            {
                for (int i = 0; i < NUM_EVENTS; i++)
                {
                    using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
                    var messageBody = $"Test message {i} - {eventHubName} - {DateTimeOffset.Now}";
                    var messageBytes = Encoding.UTF8.GetBytes(messageBody);
                    var eventData = new EventData(messageBytes);

                    eventBatch.TryAdd(eventData);
                    await producerClient.SendAsync(eventBatch);
                    await Task.Delay(TimeSpan.FromSeconds(2));
                }
            }

            Console.WriteLine($"All events sent to - {eventHubName}");
        }


        private static async Task<EventProcessorClient> RegisterEventReciever(string eventHubConnectionString,
           string eventHubName,
           string blobStorageConnectionString,
           string blobContainerName)
        {
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
            BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);
            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, eventHubConnectionString, eventHubName);

            processor.ProcessEventAsync += ProcessEventHubMessageHandler;
            processor.ProcessErrorAsync += ProcessEventHubMessageErrorHandler;

            await processor.StartProcessingAsync();

            return processor;
        }

        private static async Task ProcessEventHubMessageHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        private static async Task ProcessEventHubMessageErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            await Task.CompletedTask;
        }
    }
}
