namespace HDBCLI
{
    using System;
    using System.IO;

    class FileOuput : TextOutput
    {
        public FileOuput(string lineSeparator, string colSeparator)
        : base(lineSeparator, colSeparator)
        {
            this.DataLineReady += HandleDataLineOutput;
        }

        public override void BeforeWrite()
        {
            while (string.IsNullOrWhiteSpace(m_fileName))
            {
                ConsoleHelper.SystemOutput("Input file name:");
                m_fileName = Console.ReadLine();
            }
            string fullFileName = GetFullFileName();
            if (File.Exists(fullFileName))
            {
                File.Delete(fullFileName);
            }
        }

        public override void AfterWrite()
        {
            ConsoleHelper.SystemOutput("Done.", true, true);
        }

        private void HandleDataLineOutput(object sender, DataLineReadyEventArgs e)
        {
            bool needWriteFile = !e.HasNext || e.DataBlock.Length > 1024 * 1024;
            if (needWriteFile)
            {
                string fullFileName = GetFullFileName();
                File.AppendAllText(fullFileName, e.DataBlock.ToString());
                e.DataBlock.Clear();
            }
        }

        private string GetFullFileName()
        {
            return Path.Combine(AppDomain.CurrentDomain.BaseDirectory, m_fileName);
        }

        private string m_fileName = null;
    }
}