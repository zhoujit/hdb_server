namespace HDBPublic
{
    class ColumnDefinition
    {
        public string Name;

        public DataType DataType;

        public bool PK;

        public string CompressType;

        public ColumnDefinition(string name, DataType dataType, bool pk, string compressType)
        {
            Name = name;
            DataType = dataType;
            PK = pk;
            CompressType = compressType;
        }

    }
}
