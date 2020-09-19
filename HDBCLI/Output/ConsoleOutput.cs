namespace HDBCLI
{
    using System;
    using System.IO;

    class ConsoleOutput : TextOutput
    {
        public ConsoleOutput(string lineSeparator, string colSeparator)
        : base(lineSeparator, colSeparator)
        {
            this.DataLineReady += HandleDataLineOutput;
        }

        private void HandleDataLineOutput(object sender, DataLineReadyEventArgs e)
        {
            ConsoleColor color = e.IsHeader ? ConsoleHelper.ResultHeaderOutputColor : ConsoleHelper.ResultOutputColor;
            ConsoleHelper.OutputText(color, e.DataBlock, false);
            e.DataBlock.Clear();
        }
    }
}
