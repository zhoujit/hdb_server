namespace HDBCLI
{
    using System;
    using System.Collections.Generic;

    class ArgumentParser
    {
        public ArgumentParser(string[] args)
        {
            // -hostname 127.0.0.1
            // --hostname 127.0.0.1
            m_argumentMap = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            for (int argumentIndex = 0; argumentIndex < args.Length / 2; argumentIndex++)
            {
                string key = args[2 * argumentIndex];
                string value = args[2 * argumentIndex + 1];
                if (key.StartsWith("-"))
                {
                    m_argumentMap.Add(key.Trim('-'), value);
                }
            }
        }

        public string this[string name, bool isRequire]
        {
            get
            {
                string temp = "";
                if (!m_argumentMap.ContainsKey(name))
                {
                    if (isRequire)
                    {
                        throw new ArgumentNullException($"No argument: {name}");
                    }
                }
                else
                {
                    temp = m_argumentMap[name];
                }
                return temp;
            }
        }


        private Dictionary<string, string> m_argumentMap;

    }
}