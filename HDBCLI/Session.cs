namespace HDBCLI
{
    enum OutputTypeEnum
    {
        Console = 1,
        CSV = 2,
        TextFile = 3,
    }

    class Session
    {
        public OutputTypeEnum OutputType { set; get; } = OutputTypeEnum.Console;

        public bool OutputCompactMode { set; get; } = false;


    }
}