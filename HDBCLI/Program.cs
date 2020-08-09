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
            string message;
            DbClient dbClient = new DbClient("192.168.56.1", 9898);
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
                {"SecId", "SEC001"}
            };
            Dictionary<string, object> sdk001 = new Dictionary<string, object>()
            {
                {"SecId", "SDK001"}
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
