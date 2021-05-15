namespace HDBPublic
{
    using System;
    using System.Collections.Generic;

    record ColumnDefinition(string Name, DataType DataType, bool PK, string CompressType);

    record AggregateInfo(string FieldName, string AsName, string AggregateType);

    record FieldInfo(string FieldName, string AsName);

    record QueryInfo(string TableName,
        List<Dictionary<string, Tuple<Object, PredicateType>>> FieldConditions,
        int? Limit = null,
        List<AggregateInfo> AggregateInfos = null,
        string[] GroupBys = null,
        List<FieldInfo> RawFieldInfos = null,
        List<string> OutputFields = null);

}
