namespace HDBPublic
{
    public class ColumnDefinition
    {
        public string Name;

        public DataType DataType;

        public bool PK;

        public ColumnDefinition(string name, DataType dataType, bool pk)
        {
            Name = name;
            DataType = dataType;
            PK = pk;
        }

    }
}
