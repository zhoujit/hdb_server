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

        public override void Prepare()
        {
            while (string.IsNullOrWhiteSpace(m_fileName))
            {
                Console.WriteLine("Input file name:");
                m_fileName = Console.ReadLine();
            }
        }

        private void HandleDataLineOutput(object sender, DataLineReadyEventArgs e)
        {
            bool needWriteFile = !e.HasNext || e.DataBlock.Length > 1024 * 1024;
            if (needWriteFile)
            {
                string fullFileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, m_fileName);
                File.AppendAllText(fullFileName, e.DataBlock.ToString());
                e.DataBlock.Clear();
            }
        }

        private string m_fileName = null;
    }
}