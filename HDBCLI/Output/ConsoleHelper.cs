namespace HDBCLI
{
    using System;
    using System.Text;

    class ConsoleHelper
    {
        public static void OutputMessage(bool success, string message, bool hasLineFeed)
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

        public static readonly ConsoleColor SystemOutputColor = ConsoleColor.DarkCyan;
        public static readonly ConsoleColor ErrorOutputColor = ConsoleColor.DarkRed;
        public static readonly ConsoleColor SuccessOutputColor = ConsoleColor.DarkMagenta;
        public static readonly ConsoleColor ResultHeaderOutputColor = ConsoleColor.DarkBlue;
        public static readonly ConsoleColor ResultOutputColor = ConsoleColor.DarkGreen;

    }
}