namespace HDBCLI
{
    using System;
    using System.Data;
    using System.Text;
    using HDBPublic;

    class HDBConsole
    {
        public HDBConsole(string hostName, int port)
        {
            m_statement = new SQLStatement(hostName, port);
            m_statement.ImpProgress += (object sender, ImpProgressEventArgs args) =>
            {
                if (args.HasNext)
                {
                    ConsoleHelper.SystemOutput($"CurrentTotalCount:{args.CurrentTotalCount}, GrandTotalCount:{args.GrandTotalCount}", true, true);
                }
                else
                {
                    ConsoleHelper.SystemOutput($"Done. GrandTotalCount:{args.GrandTotalCount}", true, true);
                }
            };

        }

        public void Run()
        {
            StringBuilder inputBuffer = new StringBuilder();
            while (true)
            {
                ConsoleHelper.OutputText(ConsoleHelper.PromptOutputColor, ">", false);

                Console.ResetColor();
                string line = Console.ReadLine();
                if (line == null)
                {
                    inputBuffer.Clear();
                    ConsoleHelper.SystemOutput("\nClear input buffer.", true, true);
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
                            else if (sql.StartsWith("set", StringComparison.CurrentCultureIgnoreCase))
                            {
                                bool success = Session.ApplyConfig(m_session, sql);
                                if (success)
                                {
                                    continue;
                                }
                            }

                            ExecuteSQL(sql);
                        }
                    }
                    catch (Exception ex)
                    {
                        string errorMessage = $"Exception:{ex.Message}\r\nStackTrace:{ex.StackTrace}";
                        ConsoleHelper.SystemOutput(errorMessage, false, true);
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
                ConsoleHelper.SystemOutput(message, success, true);
            }
        }

        private SQLStatement m_statement = null;
        private Session m_session = new Session();
    }
}