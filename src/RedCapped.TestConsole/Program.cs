using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using RedCapped.Core;

namespace RedCapped.TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Starting tests:");
            Console.WriteLine();
            Task.Run(() => MainAsync(args)).Wait();
            Console.ReadKey();
        }

        static async void MainAsync(string[] args)
        {
            var m = new QueueManager("mongodb://localhost", "redcappedtest");
            var q = await m.CreateQueueAsync<string>("test", 64*1024*1024);

            await PublishMessages(q, QoS.Low, 10);
            await PublishMessages(q, QoS.Normal, 10);
            await PublishMessages(q, QoS.High, 10);

            Console.WriteLine("Press any key to exit.");
        }

        private static async Task PublishMessages(IQueueOf<string> queue, QoS qos, int passes)
        {
            var watch = new Stopwatch();
            var counters = new List<long>();

            for (var i = 1; i <= passes; i++)
            {
                Console.Write("Publishing 1000 messages ({0} QoS), pass {1}/{2} -> ", qos.ToString(), i, passes);
                watch.Restart();

                for (var j = 0; j < 1000; j++)
                {
                    await queue.PublishAsync("test", j.ToString(), qos: qos);
                }
                watch.Stop();
                counters.Add(watch.ElapsedMilliseconds);
                Console.WriteLine(watch.ElapsedMilliseconds);
            }

            var average = counters.Sum() / passes;

            Console.WriteLine("Average time (ms): {0}", average);
            Console.WriteLine("Average rate (msg/s): {0}", Math.Truncate(decimal.Divide(1000, average) * 1000));
            Console.WriteLine();            
        }
    }
}
