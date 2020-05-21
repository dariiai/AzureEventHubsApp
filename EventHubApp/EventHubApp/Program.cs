using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace MyAzEventHub
{
    public class Program
    {
        private static int NUM_EVENTS = 10;

        static async Task Main(string[] args)
        {
            var eventHubConnectionString = @"Endpoint=sb://didieventhubns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=tsaMBLbPzSp7fbfoRMYymTkA1E8JVKgFTHeJ5Q0KlcM=";
            var eventHubName = "didieventhub";

            await SendEvents(eventHubConnectionString, eventHubName);
            await Task.Delay(TimeSpan.FromMinutes(10));          
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


    }
}
