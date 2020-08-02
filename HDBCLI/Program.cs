using System;
using HDBPublic;

namespace HDBCLI
{
    class Program
    {
        static void Main(string[] args)
        {
            string message;
            DbClient dbClient = new DbClient("192.168.56.1", 9898);
            bool success = dbClient.Hi(out message);
            Console.WriteLine($"{success}: {message}");
            Console.WriteLine("Hello World!");
        }
    }
}
