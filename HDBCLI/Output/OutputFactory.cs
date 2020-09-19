namespace HDBCLI
{
    using System;

    class OutputFactory
    {
        public static IOutput CreateInstance(OutputTypeEnum outputEnum)
        {
            IOutput result = null;
            switch (outputEnum)
            {
                case OutputTypeEnum.Console:
                    result = new ConsoleOutput("\r\n", "|");
                    break;
                case OutputTypeEnum.CSV:
                    result = new FileOuput("\r\n", ",");
                    break;
                case OutputTypeEnum.TextFile:
                    result = new FileOuput("\r\n", "\t");
                    break;

                default:
                    throw new NotImplementedException($"Not implemented output type: {outputEnum}");
            }
            return result;
        }

    }
}