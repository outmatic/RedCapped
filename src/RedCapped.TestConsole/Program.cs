using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using RedCapped.Core;
using static System.Console;

namespace RedCapped.TestConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            WriteLine("Starting tests:");
            WriteLine();

            Task.Run(() => MainAsync(args)).Wait();
            WriteLine("Press any key to exit.");
            ReadKey();
        }

        static async void MainAsync(string[] args)
        {
            var m = new QueueFactory("mongodb://localhost", "redcappedtest");
            var q = await m.CreateQueueAsync<string>("test2", 64*1024*1024);

            await PublishMessages(q, QoS.Low, 10);
            await PublishMessages(q, QoS.Normal, 10);
            await PublishMessages(q, QoS.High, 10);

            var counter = 0;

            var watch = new Stopwatch();
            watch.Start();

            q.Subscribe(msg =>
            {
                counter++;

                if (counter%1000 != 0) return true;

                Console.SetCursorPosition(0, Console.CursorTop);
                Console.Write("Receive rate (msg/s): {0}", Math.Truncate(decimal.Divide(1000, watch.ElapsedMilliseconds) * 1000));
                watch.Restart();

                return true;
            });
        }

        private static async Task PublishMessages(IQueueOf<string> queue, QoS qos, int passes)
        {
            var watch = new Stopwatch();
            var counters = new List<long>();

            for (var i = 1; i <= passes; i++)
            {
                Write("Publishing 1000 messages ({0} QoS), pass {1}/{2} -> ", qos.ToString(), i, passes);
                watch.Restart();

                for (var j = 0; j < 1000; j++)
                {
                    await queue.PublishAsync(j.ToString(), qos: qos);
                }
                watch.Stop();
                counters.Add(watch.ElapsedMilliseconds);
                WriteLine(watch.ElapsedMilliseconds);
            }

            var average = counters.Sum() / passes;

            WriteLine("Average time (ms): {0}", average);
            WriteLine("Average rate (msg/s): {0}", Math.Truncate(decimal.Divide(1000, average) * 1000));
            WriteLine();            
        }
    }
}
