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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace MessagingSamples
{
    public class Program : IBasicQueueSendReceiveSample
    {
        public async Task Run(string namespaceAddress, string queueName, string sendToken, string receiveToken)
        {
            Console.WriteLine("Press any key to exit the scenario");

            var cts = new CancellationTokenSource();

            var sendTask = SendMessagesAsync(namespaceAddress, queueName, sendToken);
            var receiveTask = ReceiveMessagesAsync(namespaceAddress, queueName, receiveToken, cts.Token);

            Console.ReadKey();
            cts.Cancel();

            await Task.WhenAll(sendTask, receiveTask);
        }

        private static async Task SendMessagesAsync(string namespaceAddress, string queueName, string sendToken)
        {
            var senderFactory = MessagingFactory.Create(
                namespaceAddress,
                new MessagingFactorySettings
                {
                    TransportType = TransportType.Amqp,
                    TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(sendToken)
                });
            var sender = await senderFactory.CreateMessageSenderAsync(queueName);

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
                var message =
                    new BrokeredMessage(scientists[i])
                    {
                        ContentType = typeof(Scientist).FullName,
                        MessageId = $"{i} / {Guid.NewGuid()}"
                    };

                await sender.SendAsync(message);

                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                    Console.ResetColor();
                }
            }
        }

        private static async Task ReceiveMessagesAsync(string namespaceAddress, string queueName, string receiveToken,
            CancellationToken cancellationToken)
        {
            var receiverFactory = MessagingFactory.Create(
                namespaceAddress,
                new MessagingFactorySettings
                {
                    TransportType = TransportType.Amqp,
                    TokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(receiveToken)
                });

            var receiver = await receiverFactory.CreateMessageReceiverAsync(queueName, ReceiveMode.PeekLock);
            
            var doneReceiving = new TaskCompletionSource<bool>();

            // close the receiver and factory when the CancellationToken fires 
            cancellationToken.Register(
                async () =>
                {
                    await receiver.CloseAsync();
                    await receiverFactory.CloseAsync();
                    doneReceiving.SetResult(true);
                });

            // register the OnMessageAsync callback
            receiver.OnMessageAsync(
                async message =>
                {
                    if (message.ContentType != null && message.ContentType.Equals(typeof(Scientist).FullName))
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
                        await message.CompleteAsync();
                    }
                    else
                    {
                        await message.DeadLetterAsync("ProcessingError", "Don't know what to do with this message");
                    }
                },
                new OnMessageOptions {AutoComplete = false, MaxConcurrentCalls = 1});

            await doneReceiving.Task;
        }
    }
}