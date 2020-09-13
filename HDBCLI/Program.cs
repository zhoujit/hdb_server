using System;
using System.Collections.Generic;
using System.Data;
using HDBPublic;

namespace HDBCLI
{
    class Program
    {
        static void Main(string[] args)
        {
            string hostName = "192.168.56.1";
            int port = 9898;

            System.AppDomain.CurrentDomain.UnhandledException += (object sender, UnhandledExceptionEventArgs args) =>
            {
                ConsoleColor foregroundColor = Console.ForegroundColor;
                try
                {
                    Console.ForegroundColor = ConsoleColor.DarkRed;
                    Exception ex = args?.ExceptionObject as Exception;
                    if (ex != null)
                    {
                        Console.WriteLine($"Exception message:{ex.Message}, IsTerminating:{args.IsTerminating}");
                    }
                    else
                    {
                        Console.WriteLine($"Exception args:{args?.ExceptionObject}");
                    }
                }
                finally
                {
                    Console.ForegroundColor = foregroundColor;
                }
            };

            new HDBConsole(hostName, port).Run();

            TestSQL(hostName, port);

            TestDbClient(hostName, port);

        }

        private static void TestSQL(string hostName, int port)
        {
            SQLStatement statement = new SQLStatement(hostName, port);

            bool success;
            string message;
            DataTable result;
            (success, message, result) = statement.Execute("insert into Issuers(Id, Name, Price) values('S2020', 'ARM 2020', 2020.12345),('S2021', 'ARM 2021', 2021.12345)");
            (success, message, result) = statement.Execute("select * from Issuers where Id = 'S2020' or Id = 'S001'");
            (success, message, result) = statement.Execute("delete from Issuers where Id='S001'");
            (success, message, result) = statement.Execute("select * from Issuers where Id = 'S001'");

        }

        static void TestDbClient(string hostName, int port)
        {
            string message;
            DbClient dbClient = new DbClient(hostName, port);
            dbClient.BeforeRequest += (sender, args) =>
            {
                Console.WriteLine("Before request. Body: " + args.RequestText);
            };
            dbClient.AfterResponse += (sender, args) =>
            {
                Console.WriteLine("After response. Body: " + args.ResponseText);
            };
            bool success = dbClient.Hi(out message);
            Console.WriteLine($"{success}: {message}");

            List<ColumnDefinition> columnDefinitions = new List<ColumnDefinition>();
            columnDefinitions.Add(new ColumnDefinition("SecId", DataType.Char, true));
            columnDefinitions.Add(new ColumnDefinition("Price", DataType.Float, false));
            dbClient.CreateTable("Issuers2", columnDefinitions);

            dbClient.GetTableList();
            dbClient.RemoveTable("Issuers2");
            dbClient.GetTableList();

            // success = dbClient.Stop(out message);
            // Console.WriteLine($"{success}: {message}");

            /*
            <Msg Op='Get' Table='Issuers'>
                <Data>
                    <SecId>SEC001</SecId>
                </Data>
                <Data>
                    <SecId>SDK001</SecId>
                </Data>
            </Msg>
            */
            Dictionary<string, object> sec001 = new Dictionary<string, object>()
            {
                {"Id", "S001"}
            };
            Dictionary<string, object> sdk001 = new Dictionary<string, object>()
            {
                {"Id", "S002"}
            };
            List<Dictionary<string, object>> fieldValues = new List<Dictionary<string, object>>()
            {
                sec001, sdk001
            };
            DataTable data = dbClient.Query("Issuers", fieldValues);
            Console.WriteLine($"{success}: {message}");

            /*
            <Msg Op='Update' Table='Issuers'>
                <Data>
                    <SecId>SEC001</SecId>
                    <Price>501.001</Price>
                </Data>
                <Data>
                    <SecId>SDK001</SecId>
                    <Price>502.001</Price>
                </Data>
            </Msg>
            */


            /*
            <Msg Op='Del' Table='Issuers'>
                <Data>
                    <SecId>SEC001</SecId>
                </Data>
                <Data>
                    <SecId>SDK001</SecId>
                </Data>
            </Msg>
            */


            /*
            <Msg Op='Add' Table='Issuers'>
                <Data>
                    <SecId>SEC001</SecId>
                    <Name>Microsoft001</Name>
                    <Price>101.123</Price>
                </Data>
                <Data>
                    <SecId>SDK001</SecId>
                    <Name>ARM001</Name>
                    <Price>301.123</Price>
                </Data>
            </Msg>
            */

        }
    }
}
