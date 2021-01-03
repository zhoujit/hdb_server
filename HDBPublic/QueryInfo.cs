namespace HDBPublic
{
    using System;
    using System.Collections.Generic;

    record ColumnDefinition(string Name, DataType DataType, bool PK, string CompressType);

    record AggregateInfo(string FieldName, string AsName, string AggregateType);

    record QueryInfo(string TableName, List<Dictionary<string, Tuple<Object, PredicateType>>> FieldConditions,
        int? Limit, List<AggregateInfo> AggregateInfos, string[] GroupBys);

}
