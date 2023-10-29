using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqliteMemoryTester
{
    class Program
    {
        static string GetRandString(int len)
        {
            var rand = new Random();
            byte[] buf = new byte[len];
            rand.NextBytes(buf);
            for (var i = 0; i < len; ++i)
            {
                buf[i] = (byte)(((buf[i] - 32) % 64) + 32);
            }
            return Encoding.UTF8.GetString(buf, 0, len);
        }

        static void Main(string[] args)
        {
            var tester = new Tester();
            tester.OpenDatabase();
            tester.CreateTables();
            //tester.GetTables();

            int message_id = 0;
            long end = DateTimeOffset.Now.ToUnixTimeSeconds() + 30;
            long checkpoint = DateTimeOffset.Now.ToUnixTimeSeconds() + 1;
            for (long now = DateTimeOffset.Now.ToUnixTimeSeconds(); now < end; now = DateTimeOffset.Now.ToUnixTimeSeconds())
            {
                int host_id = tester.AddHost(GetRandString(10));
                int program_id = tester.AddProgram(GetRandString(20));
                tester.AddLogEvent(host_id, program_id, ++message_id);
                if (now == checkpoint)
                {
                    Console.WriteLine(message_id);
                    checkpoint = DateTimeOffset.Now.ToUnixTimeSeconds() + 1;
                }
            }
            Console.WriteLine("Done.");
            Console.ReadKey();

        }
    }
}
