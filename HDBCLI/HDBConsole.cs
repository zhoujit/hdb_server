using System;
using System.Data;
using System.Text;
using HDBPublic;

namespace HDBCLI
{
    class HDBConsole
    {
        public HDBConsole(string hostName, int port)
        {
            m_statement = new SQLStatement(hostName, port);
        }

        public void Run()
        {
            StringBuilder inputBuffer = new StringBuilder();
            while (true)
            {
                OutputText(SystemOutputColor, ">", false);

                Console.ResetColor();
                string line = Console.ReadLine().TrimEnd();
                inputBuffer.AppendFormat($"\n{line}");
                if (line.EndsWith(";"))
                {
                    try
                    {
                        ExecuteSQL(inputBuffer.ToString());
                    }
                    catch (Exception ex)
                    {
                        string errorMessage = $"Exception:{ex.Message}\r\nStackTrace:{ex.StackTrace}";
                        OutputText(ErrorOutputColor, errorMessage);
                    }
                    finally
                    {
                        inputBuffer.Clear();
                    }
                }
            }
        }

        private void ExecuteSQL(string sql)
        {
            sql = sql.Trim(new char[] { ' ', ';', '\t', '\r', '\n' });
            if (sql.Length == 0)
            {
                return;
            }

            (bool success, string message, DataTable result) = m_statement.Execute(sql);
            if (result != null)
            {
                OutputResult(result);
            }
            else
            {
                OutputMessage(success, message);
            }
        }

        private void OutputMessage(bool success, string message)
        {
            ConsoleColor outputColor = success ? SuccessOutputColor : ErrorOutputColor;
            OutputText(outputColor, message);
        }

        private void OutputResult(DataTable result)
        {
            StringBuilder outputBuffer = new StringBuilder();
            foreach (DataColumn dataColumn in result.Columns)
            {
                if (outputBuffer.Length > 0)
                {
                    outputBuffer.Append("|");
                }
                outputBuffer.Append(dataColumn.ColumnName);
            }
            OutputText(ResultOutputColor, outputBuffer);

            foreach (DataRow row in result.Rows)
            {
                bool firstColumn = true;
                foreach (DataColumn dataColumn in result.Columns)
                {
                    if (firstColumn)
                    {
                        firstColumn = false;
                    }
                    else
                    {
                        outputBuffer.Append("|");
                    }
                    FormatValue(outputBuffer, row, dataColumn);
                }

                OutputText(ResultOutputColor, outputBuffer);
            }
        }

        private void FormatValue(StringBuilder outputBuffer, DataRow row, DataColumn dataColumn)
        {
            if (!row.IsNull(dataColumn))
            {
                outputBuffer.Append(row[dataColumn]);
            }
        }

        private void OutputText(ConsoleColor color, StringBuilder outputBuffer)
        {
            OutputText(color, outputBuffer.ToString());
            outputBuffer.Clear();
        }

        private void OutputText(ConsoleColor color, string message, bool hasLineFeed = true)
        {
            if (color != Console.ForegroundColor)
            {
                Console.ForegroundColor = color;
            }
            if (hasLineFeed)
            {
                Console.WriteLine(message);
            }
            else
            {
                Console.Write(message);
            }
        }

        private static readonly ConsoleColor SystemOutputColor = ConsoleColor.DarkCyan;
        private static readonly ConsoleColor ErrorOutputColor = ConsoleColor.DarkRed;
        private static readonly ConsoleColor SuccessOutputColor = ConsoleColor.Green;
        private static readonly ConsoleColor ResultOutputColor = ConsoleColor.DarkBlue;

        private SQLStatement m_statement = null;
    }
}