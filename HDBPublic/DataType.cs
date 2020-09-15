using System;
using System.Collections.Generic;

namespace HDBPublic
{
    enum DataType
    {
        Char,
        Varchar,
        Byte,
        Short,
        Int,
        Long,
        Float,
        Double,
    }

    class DataTypeHelper
    {
        public static bool TryParse(string dataTypeString, out DataType dataType)
        {
            bool success = false;
            dataType = DataType.Byte;
            if (m_dataTypeMap.ContainsKey(dataTypeString))
            {
                dataType = m_dataTypeMap[dataTypeString];
                success = true;
            }
            return success;
        }

        private readonly static Dictionary<string, DataType> m_dataTypeMap = new Dictionary<string, DataType>(StringComparer.CurrentCultureIgnoreCase){
            {"Char", DataType.Char},
            {"Varchar", DataType.Varchar},
            {"Byte", DataType.Byte},
            {"Short", DataType.Short},
            {"Int", DataType.Int},
            {"Long", DataType.Long},
            {"Float", DataType.Float},
            {"Double", DataType.Double},
        };
    }
}
