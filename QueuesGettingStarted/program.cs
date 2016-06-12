//   
//   Copyright © Microsoft Corporation, All Rights Reserved
// 
//   Licensed under the Apache License, Version 2.0 (the "License"); 
//   you may not use this file except in compliance with the License. 
//   You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0 
// 
//   THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS
//   OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION
//   ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A
//   PARTICULAR PURPOSE, MERCHANTABILITY OR NON-INFRINGEMENT.
// 
//   See the Apache License, Version 2.0 for the specific language
//   governing permissions and limitations under the License. 

using System;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace MessagingSamples
{
    public class Program : IBasicQueueConnectionStringSample
    {
        private QueueClient receiveClient;
        private QueueClient sendClient;

        public async Task Run(string queueName, string connectionString)
        {
            Console.WriteLine("Press any key to exit the scenario");

            receiveClient = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);
            InitializeReceiver();

            sendClient = QueueClient.CreateFromConnectionString(connectionString, queueName);
            var sendTask = SendMessagesAsync();

            Console.ReadKey();

            // shut down the receiver, which will stop the OnMessageAsync loop
            await receiveClient.CloseAsync();

            // wait for send work to complete if required
            await sendTask;

            await sendClient.CloseAsync();
        }

        private async Task SendMessagesAsync()
        {
            var scientists = new[]
            {
                new Scientist {Name = "Einstein", FirstName = "Albert"},
                new Scientist {Name = "Heisenberg", FirstName = "Werner"},
                new Scientist {Name = "Curie", FirstName = "Marie"},
                new Scientist {Name = "Hawking", FirstName = "Steven"},
                new Scientist {Name = "Newton", FirstName = "Isaac"},
                new Scientist {Name = "Bohr", FirstName = "Niels"},
                new Scientist {Name = "Faraday", FirstName = "Michael"},
                new Scientist {Name = "Galilei", FirstName = "Galileo"},
                new Scientist {Name = "Kepler", FirstName = "Johannes"},
                new Scientist {Name = "Kopernikus", FirstName = "Nikolaus"}
            };


            for (var i = 0; i < scientists.Length; i++)
            {
                var scientist = scientists[i];

                //var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i]))))

                lock (Console.Out)
                {
                    Console.WriteLine($"Creating message: Id = {i}");
                }

                var message = new BrokeredMessage(scientist)
                {
                    Label = "Scientist",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                lock (Console.Out)
                {
                    Console.WriteLine($"Sending message: Id = {message.MessageId}");
                }

                await sendClient.SendAsync(message);

                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"Message sent: Id = {message.MessageId}");
                    Console.ResetColor();
                }
            }
        }

        private void InitializeReceiver()
        {
            // register the OnMessageAsync callback
            receiveClient.OnMessageAsync(
                async message =>
                {
                    if (message.ContentType == null &&
                        message.Label != null &&
                        message.Label.Equals("Scientist", StringComparison.InvariantCultureIgnoreCase))
                    {
                        var scientist = message.GetBody<Scientist>();

                        lock (Console.Out)
                        {
                            Console.ForegroundColor = ConsoleColor.Cyan;
                            Console.WriteLine(
                                "\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = {0}, \n\t\t\t\t\t\tSequenceNumber = {1}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {2}," +
                                "\n\t\t\t\t\t\tExpiresAtUtc = {5}, \n\t\t\t\t\t\tContentType = \"{3}\", \n\t\t\t\t\t\tSize = {4},  \n\t\t\t\t\t\tContent: [ firstName = {6}, name = {7} ]",
                                message.MessageId,
                                message.SequenceNumber,
                                message.EnqueuedTimeUtc,
                                message.ContentType,
                                message.Size,
                                message.ExpiresAtUtc,
                                scientist.FirstName,
                                scientist.Name);
                            Console.ResetColor();
                        }
                    }
                    else
                    {
                        lock (Console.Out)
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine(
                                "\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = {0}, \n\t\t\t\t\t\tSequenceNumber = {1}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {2}," +
                                "\n\t\t\t\t\t\tExpiresAtUtc = {5}, \n\t\t\t\t\t\tContentType = \"{3}\", \n\t\t\t\t\t\tSize = {4},  \n\t\t\t\t\t\tCANNOT PROCESS BECAUSE RULE DOES NOT MATCH",
                                message.MessageId,
                                message.SequenceNumber,
                                message.EnqueuedTimeUtc,
                                message.ContentType,
                                message.Size,
                                message.ExpiresAtUtc);
                            Console.ResetColor();
                        }
                    }
                    await message.CompleteAsync();
                },
                new OnMessageOptions {AutoComplete = false, MaxConcurrentCalls = 1});
        }
    }
}