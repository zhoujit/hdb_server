namespace HDBPublic
{
    enum OpType
    {
        InvalidOp = 0,
        Get = 1,
        Add = 2,
        Update = 3,
        Del = 4,

        GetTableList = 101,
        CreateTable = 102,
        RemoveTable = 103,
        TruncateTable = 104,
    }
}
