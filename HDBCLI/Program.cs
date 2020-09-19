using System;

namespace HDBCLI
{
    class Program
    {
        static void Main(string[] args)
        {
            System.AppDomain.CurrentDomain.UnhandledException += (object sender, UnhandledExceptionEventArgs args) =>
            {
                Exception ex = args?.ExceptionObject as Exception;
                if (ex != null)
                {
                    Console.WriteLine($"Exception message:{ex.Message}\r\nStackTrace:{ex.StackTrace}\r\nIsTerminating:{args.IsTerminating}.");
                }
                else
                {
                    Console.WriteLine($"Exception args:{args?.ExceptionObject}.");
                }
            };

            ArgumentParser argumentParser = new ArgumentParser(args);
            string hostName = argumentParser["hostName", true];
            int port = int.Parse(argumentParser["port", true]);

            new HDBConsole(hostName, port).Run();
        }

        

    }
}
