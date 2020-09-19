namespace HDBCLI
{
    enum OutputTypeEnum
    {
        Console = 1,
        CSV = 2,
        TabFile = 3,
        TextFile = 4,

    }

    class Session
    {
        public OutputTypeEnum OutputType { set; get; } = OutputTypeEnum.Console;

        public bool OutputCompactMode { set; get; } = false;


    }
}