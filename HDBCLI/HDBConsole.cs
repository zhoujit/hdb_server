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
                ConsoleHelper.OutputMessage(true, ">");

                Console.ResetColor();
                string line = Console.ReadLine();
                if (line == null)
                {
                    inputBuffer.Clear();
                    ConsoleHelper.OutputMessage(true, "\nClear input buffer.");
                    continue;
                }
                line = line.TrimEnd();
                inputBuffer.AppendFormat($"\n{line}");
                if (line.EndsWith(";"))
                {
                    try
                    {
                        string sql = inputBuffer.ToString();
                        sql = sql.Trim(new char[] { ' ', ';', '\t', '\r', '\n' });
                        if (sql.Length > 0)
                        {
                            if (string.Compare(sql, "exit", true) == 0)
                            {
                                Environment.Exit(0);
                            }

                            ExecuteSQL(sql);
                        }
                    }
                    catch (Exception ex)
                    {
                        string errorMessage = $"Exception:{ex.Message}\r\nStackTrace:{ex.StackTrace}";
                        ConsoleHelper.OutputMessage(false, errorMessage);
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
            (bool success, string message, DataTable result) = m_statement.Execute(sql);
            if (result != null)
            {
                IOutput output = OutputFactory.CreateInstance(m_session.OutputType);
                output.Write(result, m_session);
            }
            else
            {
                ConsoleHelper.OutputMessage(success, message);
            }
        }

        private SQLStatement m_statement = null;
        private Session m_session = new Session();
    }
}