namespace HDBCLI
{
    using System;
    using System.Text.RegularExpressions;

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

        public static bool ApplyConfig(Session session, string config)
        {
            Regex regexOutput = new Regex(@"set\s+output\s+(?<OutputOption>.+)", RegexOptions.Singleline | RegexOptions.IgnoreCase);
            Match match = regexOutput.Match(config);
            bool success = match.Success;
            if (success)
            {
                string outputOption = match.Groups["OutputOption"].Value.Trim();
                OutputTypeEnum outputTypeEnum;
                success = Enum.TryParse<OutputTypeEnum>(outputOption, true, out outputTypeEnum);
                if (success)
                {
                    session.OutputType = outputTypeEnum;
                }
                else if (string.Compare(outputOption, "compress", true) == 0)
                {
                    success = true;
                    session.OutputCompactMode = true;
                }
                else if (string.Compare(outputOption, "uncompress", true) == 0)
                {
                    success = true;
                    session.OutputCompactMode = false;
                }
            }

            return success;
        }
    }
}