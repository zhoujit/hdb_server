namespace HDBCLI
{
    using System;
    using System.Text;

    class ConsoleHelper
    {
        public static void SystemOutput(string message, bool success = true, bool hasLineFeed = false)
        {
            ConsoleColor outputColor = success ? SuccessOutputColor : ErrorOutputColor;
            OutputText(outputColor, message, hasLineFeed);
        }

        public static void OutputText(ConsoleColor color, StringBuilder outputBuffer, bool hasLineFeed)
        {
            OutputText(color, outputBuffer.ToString(), hasLineFeed);
            outputBuffer.Clear();
        }

        public static void OutputText(ConsoleColor color, string message, bool hasLineFeed)
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

        private static readonly ConsoleColor ErrorOutputColor = ConsoleColor.DarkRed;
        private static readonly ConsoleColor SuccessOutputColor = ConsoleColor.DarkMagenta;
        public static readonly ConsoleColor ResultHeaderOutputColor = ConsoleColor.DarkBlue;
        public static readonly ConsoleColor ResultOutputColor = ConsoleColor.DarkGreen;
        public static readonly ConsoleColor PromptOutputColor = ConsoleColor.Red;

    }
}