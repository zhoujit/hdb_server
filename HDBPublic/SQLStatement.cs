namespace HDBPublic
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics;
    using System.Text.RegularExpressions;


    public partial class SQLStatement
    {
        public SQLStatement(string hostName, int port)
        {
            m_dbClient = new DbClient(hostName, port);
        }

        public (bool success, string message, DataTable result) Execute(string sql)
        {
            Stopwatch stepTime = new Stopwatch();
            bool success = false;
            string message = "";
            DataTable result = null;
            if (sql.StartsWith("select", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = null;
                foreach (var selectRegex in SelectRegexes)
                {
                    match = selectRegex.Match(sql);
                    success = match.Success;
                    if (success)
                    {
                        break;
                    }
                }

                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string whereStatement = match.Groups["Where"].Value.Trim();
                    string groupByStatement = match.Groups["GroupByList"].Value.Trim();
                    string fieldListStatement = match.Groups["FieldList"].Value.Trim();
                    string limitString = "";
                    if (match.Groups["Limit"].Success)
                    {
                        limitString = match.Groups["Limit"].Value;
                    }
                    else if (match.Groups["TopN"].Success)
                    {
                        limitString = match.Groups["TopN"].Value;
                    }
                    int? limit = null;
                    if (!string.IsNullOrWhiteSpace(limitString))
                    {
                        int temp;
                        if (int.TryParse(limitString, out temp))
                        {
                            if (temp > 0)
                            {
                                limit = temp;
                            }
                            else
                            {
                                throw new Exception("Limit/top must greater than 0.");
                            }
                        }
                    }

                    var fieldConditions = ParseWhereStatement(whereStatement);
                    (var aggregateInfos, var fieldInfos) = ParseAggregateStatement(fieldListStatement);
                    var groupBys = groupByStatement.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                    CheckFieldInfoForGroupBy(fieldInfos, groupBys);
                    QueryInfo queryInfo = new QueryInfo(tableName, fieldConditions, limit, aggregateInfos, groupBys, fieldInfos);

                    stepTime.Start();
                    result = m_dbClient.Query(queryInfo);
                    stepTime.Stop();

                    message = $"Returned {result.Rows.Count} record(s). Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s\n";
                }
                else
                {
                    message = $@"Invalid select statement:{sql}
Memo:
1. Select statement must contains where clause, and field list must be *
2. Two consecutive single-quotes in string, which are treated as a literal single-quote

Example:
select * from t1 where f1 = 100;
";
                }
            }
            else if (sql.StartsWith("insert", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = InsertRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string fieldNameList = match.Groups["FieldNameList"].Value;
                    string fieldValueList = match.Groups["FieldValueList"].Value;

                    List<Dictionary<string, Tuple<Object, PredicateType>>> fieldConditions = new List<Dictionary<string, Tuple<Object, PredicateType>>>();
                    success = FillFieldValue(fieldNameList, fieldValueList, fieldConditions, out message);
                    if (success)
                    {
                        QueryInfo queryInfo = new QueryInfo(tableName, fieldConditions, null, null, null, null);
                        stepTime.Start();
                        m_dbClient.Add(queryInfo);
                        stepTime.Stop();

                        message = $"Succeed to append record(s). Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s";
                    }
                }
                else
                {
                    message = $"Invalid select statement:{sql}";
                }
            }
            else if (sql.StartsWith("delete", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = DeleteRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string whereStatement = match.Groups["Where"].Value.Trim();

                    List<Dictionary<string, Tuple<Object, PredicateType>>> fieldConditions = ParseWhereStatement(whereStatement);
                    QueryInfo queryInfo = new QueryInfo(tableName, fieldConditions, null, null, null, null);

                    stepTime.Start();
                    m_dbClient.Delete(queryInfo);
                    stepTime.Stop();

                    message = $"Succeed to remove record(s). Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s";
                }
                else
                {
                    message = $"Invalid delete statement:{sql}";
                }
            }
            else if (sql.StartsWith("drop", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = DropTableRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    stepTime.Start();
                    m_dbClient.RemoveTable(tableName);
                    stepTime.Stop();
                    message = $"Succeed to drop table. Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s";
                }
                else
                {
                    message = $"Invalid drop table statement:{sql}";
                }
            }
            else if (sql.StartsWith("truncate", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = TruncateTableRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    stepTime.Start();
                    m_dbClient.TruncateTable(tableName);
                    stepTime.Stop();
                    message = $"Succeed to truncate table. Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s";
                }
                else
                {
                    message = $"Invalid truncate table statement:{sql}";
                }
            }
            else if (sql.StartsWith("create", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = CreateTableRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    List<ColumnDefinition> columnDefinitions = new List<ColumnDefinition>();
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string fieldDefList = match.Groups["FieldDefList"].Value.Trim();
                    string[] fieldDefs = fieldDefList.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
                    foreach (string fieldDef in fieldDefs)
                    {
                        string[] fieldItems = fieldDef.Split(new char[] { ' ', '\r', '\n', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        if (fieldItems.Length >= 2 && fieldItems.Length <= 4)
                        {
                            string fieldName = fieldItems[0];
                            string dataTypeString = fieldItems[1];
                            bool pk = false;
                            string compressType = "";
                            HashSet<string> set = new HashSet<string>();
                            if (fieldItems.Length > 2)
                            {
                                set.Add(fieldItems[2].ToLower());
                            }
                            if (fieldItems.Length > 3)
                            {
                                set.Add(fieldItems[3].ToLower());
                            }
                            if (set.Contains("pk"))
                            {
                                pk = true;
                            }
                            if (set.Contains("lz4"))
                            {
                                compressType = "lz4";
                            }

                            if (!FieldNameRegex.Match(fieldName).Success)
                            {
                                success = false;
                                message = $"Invalid field name: {fieldName}";
                                break;
                            }
                            DataType dataType;
                            if (!DataTypeHelper.TryParse(dataTypeString, out dataType))
                            {
                                success = false;
                                message = $"Invalid data type: {dataTypeString}";
                                break;
                            }
                            ColumnDefinition columnDefinition = new ColumnDefinition(fieldName, dataType, pk, compressType);
                            columnDefinitions.Add(columnDefinition);
                        }
                        else
                        {
                            success = false;
                            message = $"Invalid column definition: {fieldDef}";
                            break;
                        }
                    }

                    if (success)
                    {
                        stepTime.Start();
                        m_dbClient.CreateTable(tableName, columnDefinitions);
                        stepTime.Stop();
                        message = $"Succeed to create table. Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s";
                    }
                }
                else
                {
                    message = $"Invalid create table statement:{sql}";
                }
            }
            else if (sql.StartsWith("show", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = ShowRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string showValue = match.Groups["ShowValue"].Value.Trim();
                    if (string.Compare(showValue, "tables", true) == 0)
                    {
                        stepTime.Start();
                        result = m_dbClient.GetTableList();
                        stepTime.Stop();
                    }
                    else
                    {
                        success = false;
                        message = $"Undefined show clause: {showValue}";
                    }
                }
                else
                {
                    message = $"Invalid show statement:{sql}";
                }
            }
            else if (sql.StartsWith("imp", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = ImportTableRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string fileName = match.Groups["FileName"].Value.Trim();
                    stepTime.Start();
                    ImportTable(tableName, fileName);
                    stepTime.Stop();
                    message = $"Succeed to import data. Elapsed:{stepTime.Elapsed.TotalSeconds.ToString("0.##")}s";
                }
                else
                {
                    message = $"Invalid imp statement:{sql}";
                }
            }
            else if (sql.StartsWith("server", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = ServerImportTableRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string fileName = match.Groups["FileName"].Value.Trim();
                    string logFileName = match.Groups["LogFileName"].Value.Trim();
                    m_dbClient.ServerImportTable(tableName, fileName, logFileName);
                    message = "It is running in backend. Please check log file in the server.";
                }
                else
                {
                    message = $"Invalid imp statement:{sql}";
                }
            }
            else if (string.Compare(sql, "stop", true) == 0)
            {
                string stopMessage;
                m_dbClient.Stop(out stopMessage);
                Console.WriteLine("Succeed to stop HDB server.");
                Environment.Exit(0);
            }
            //else if (sql.StartsWith("update", StringComparison.CurrentCultureIgnoreCase))
            //{
            //}
            else
            {
                return (false, "Unidentified SQL:\n" + sql, null);
            }
            return (success, message, result);
        }

        private void CheckFieldInfoForGroupBy(List<FieldInfo> fieldInfos, string[] groupBys)
        {
            if (groupBys?.Length > 0 && fieldInfos?.Count > 0)
            {
                HashSet<string> groupFieldSet = new HashSet<string>(groupBys);
                foreach (var fieldInfo in fieldInfos)
                {
                    if (!groupFieldSet.Contains(fieldInfo.FieldName))
                    {
                        throw new ArgumentException($"This field {fieldInfo.FieldName} is not in group by list.");
                    }
                }
            }
        }

        private Tuple<List<AggregateInfo>, List<FieldInfo>> ParseAggregateStatement(string aggregateStatement)
        {
            List<AggregateInfo> aggregateInfos = new List<AggregateInfo>();
            List<FieldInfo> fieldInfos = new List<FieldInfo>();
            foreach (string aggregateLine in aggregateStatement.Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                string temp = aggregateLine.Trim();
                Match match = AggregateLineRegex.Match(temp);
                if (temp.Contains("(") || temp.Contains(")"))
                {
                    if (!match.Success)
                    {
                        throw new ArgumentException($"Invalid aggregate function:{temp}.");
                    }
                    string fieldName = match.Groups["FieldName"].Value;
                    string asName = match.Groups["AsName"].Value;
                    string aggregateType = match.Groups["AggregateType"].Value;
                    aggregateInfos.Add(new AggregateInfo(fieldName, asName, aggregateType));
                }
                else
                {
                    const string AS = " as ";
                    if (temp.Contains(AS))
                    {
                        throw new ArgumentException($"Not supported 'as' in select list.");
                        // string[] items = temp.Split(AS, StringSplitOptions.RemoveEmptyEntries);
                        // if (items.Length == 2)
                        // {
                        //     fieldInfos.Add(new FieldInfo(items[0], items[1]));
                        // }
                        // else
                        // {
                        //     throw new ArgumentException($"Invalid field:{temp} in select list.");
                        // }
                    }
                    else
                    {
                        fieldInfos.Add(new FieldInfo(temp, temp));
                    }
                }
            }
            return new Tuple<List<AggregateInfo>, List<FieldInfo>>(aggregateInfos, fieldInfos);
        }

        private List<Dictionary<string, Tuple<Object, PredicateType>>> ParseWhereStatement(string whereStatement)
        {
            List<Dictionary<string, Tuple<Object, PredicateType>>> result = new List<Dictionary<string, Tuple<Object, PredicateType>>>();
            whereStatement = whereStatement + " ";
            List<char> splitterChars = new List<char>() { ' ', '\t', '\r', '\n' };
            List<char> predicateChars = new List<char>() { '=', '<', '>' };
            Dictionary<string, Tuple<Object, PredicateType>> filterLine = new Dictionary<string, Tuple<Object, PredicateType>>();
            bool isName = true;
            PredicateType predicateType = PredicateType.EQ;
            int? startIndex = null;
            int? endIndex = null;
            string lastFieldName = null;
            for (int i = 0; i < whereStatement.Length; i++)
            {
                char c = whereStatement[i];
                if (isName)
                {
                    // Field name.
                    if (startIndex == null)
                    {
                        if (!splitterChars.Contains(c))
                        {
                            startIndex = i;
                            lastFieldName = null;
                        }

                        if (c == '=')
                        {
                            throw new Exception("Field name cannot be empty.");
                        }
                        continue;
                    }

                    if (splitterChars.Contains(c) || predicateChars.Contains(c))
                    {
                        endIndex = i;

                        lastFieldName = whereStatement.Substring(startIndex.Value, endIndex.Value - startIndex.Value).Trim();

                        startIndex = null;
                        endIndex = null;
                        isName = false;

                        // Locate next word
                        while (i < whereStatement.Length && splitterChars.Contains(whereStatement[i]))
                        {
                            i++;
                        }
                        string nextWord = "";
                        int j = i;
                        // Find =, <, <=, >, >=
                        while (j < whereStatement.Length && predicateChars.Contains(whereStatement[j]))
                        {
                            nextWord += whereStatement[j];
                            j++;
                        }

                        if (nextWord.Length == 0)
                        {
                            // Find like.
                            j = i;
                            while (j < whereStatement.Length && !splitterChars.Contains(whereStatement[j]))
                            {
                                nextWord += whereStatement[j];
                                j++;
                            }
                        }

                        if (PredicateTypeHelper.TryParse(nextWord, out predicateType))
                        {
                            if (predicateType == PredicateType.LIKE)
                            {
                                i = j;
                            }
                            else
                            {
                                i = j - 1;
                            }
                            continue;
                        }
                        else
                        {
                            throw new Exception($"Invalid predicate type: {nextWord}");
                        }
                    }
                }
                else
                {
                    // Field value.
                    if (startIndex == null)
                    {
                        if (!splitterChars.Contains(c))
                        {
                            startIndex = i;
                        }
                        continue;
                    }

                    if ((splitterChars.Contains(c) && whereStatement[startIndex.Value] != SingleQuotation) || c == SingleQuotation)
                    {
                        if (c == SingleQuotation)
                        {
                            if (whereStatement[startIndex.Value] != SingleQuotation)
                            {
                                throw new Exception("String must in single quotation.");
                            }

                            if (i < whereStatement.Length - 1 && whereStatement[i + 1] == SingleQuotation)
                            {
                                // The first single-quotation in doule single-quotation.
                                continue;
                            }

                            int quotationCount = 0;
                            int tempIdx = i;
                            while (tempIdx > startIndex)
                            {
                                if (whereStatement[tempIdx] == SingleQuotation)
                                {
                                    quotationCount++;
                                }
                                else
                                {
                                    break;
                                }
                                tempIdx--;
                            }
                            if (quotationCount % 2 == 0)
                            {
                                // The second single-quotation in doule single-quotation.
                                // 'What''s your name'
                                continue;
                            }

                        }

                        endIndex = i;

                        string fieldValue = whereStatement.Substring(startIndex.Value, endIndex.Value - startIndex.Value).Trim();
                        if (!fieldValue.StartsWith("'")
                            && (fieldValue.Contains("\r") || fieldValue.Contains("\n"))
                            )
                        {
                            throw new Exception("Place single quotation around the character when there is a carriage return/line feed in select statement.");
                        }

                        if (fieldValue.StartsWith("\'"))
                        {
                            fieldValue = fieldValue.Substring(1);
                        }
                        filterLine.Add(lastFieldName, new Tuple<object, PredicateType>(fieldValue.Replace("\'\'", "\'"), predicateType));

                        startIndex = null;
                        endIndex = null;
                        isName = true;
                        lastFieldName = null;

                        i++;
                        while (i < whereStatement.Length)
                        {
                            if (splitterChars.Contains(whereStatement[i]))
                            {
                                i++;
                                continue;
                            }
                            break;
                        }
                        if (i >= whereStatement.Length)
                        {
                            break;
                        }

                        if (i < whereStatement.Length - 2 && string.Compare("OR", whereStatement.Substring(i, 2), true) == 0)
                        {
                            if (filterLine.Count > 0)
                            {
                                result.Add(filterLine);
                                filterLine = new Dictionary<string, Tuple<object, PredicateType>>();
                            }

                            i += 2;
                            continue;
                        }
                        else if (i < whereStatement.Length - 3 && string.Compare("AND", whereStatement.Substring(i, 3), true) != 0)
                        {
                            throw new Exception("Invalid where clause.");
                        }

                        i += 3;

                    }

                }
            }

            if (lastFieldName != null)
            {
                throw new Exception("Incompleted where clause.");
            }

            if (filterLine.Count > 0)
            {
                result.Add(filterLine);
            }
            return result;
        }

        private bool FillFieldValue(string fieldNameList, string fieldValueList, List<Dictionary<string, Tuple<Object, PredicateType>>> valueSet, out string errorMessage)
        {
            bool result = true;
            errorMessage = "";

            string[] fieldNames = fieldNameList.Split(new char[] { ',' });
            for (int i = 0; i < fieldNames.Length; i++)
            {
                if (fieldNames[i].Trim().Length == 0)
                {
                    errorMessage = "Field name cannot be empty.\n" + fieldNameList;
                    result = false;
                    break;
                }
            }

            if (result)
            {
                int? nextIndex = 0;
                while (nextIndex != null && nextIndex < fieldValueList.Length)
                {
                    int startIndex = nextIndex.Value;
                    List<string> fieldValues = ParseFieldValueList(fieldValueList, startIndex, out nextIndex);

                    if (result && fieldNames.Length != fieldValues.Count)
                    {
                        errorMessage = $"Mismatched between field name and value list.\n{fieldNameList}\n{fieldValueList}";
                        result = false;
                    }

                    if (result)
                    {
                        Dictionary<string, Tuple<Object, PredicateType>> fieldNameValues = new Dictionary<string, Tuple<Object, PredicateType>>();
                        for (int i = 0; i < fieldNames.Length; i++)
                        {
                            fieldNameValues.Add(fieldNames[i].Trim(), new Tuple<Object, PredicateType>(fieldValues[i], PredicateType.EQ));
                        }
                        if (fieldNameValues.Count > 0)
                        {
                            valueSet.Add(fieldNameValues);
                        }
                    }
                }
            }

            return result;
        }

        private static List<string> ParseFieldValueList(string line, int currentIndex, out int? nextIndex)
        {
            nextIndex = null;
            List<string> columnValues = new List<string>();
            bool escaped = false;
            int startIndex = 0;
            int? endIndex = null;
            bool canSkipSpace = true;  // only for prefix space
            for (; currentIndex < line.Length; currentIndex++)
            {
                char c = line[currentIndex];
                if (c == SingleQuotation)
                {
                    if (escaped)
                    {
                        if (currentIndex == line.Length - 1)
                        {
                            // last quotation mark
                            endIndex = currentIndex;
                        }
                        else if (line[currentIndex + 1] == SingleQuotation)
                        {
                            // '' -> '
                            currentIndex++;
                            continue;
                        }
                        else
                        {
                            endIndex = currentIndex;
                            for (int j = currentIndex + 1; j < line.Length; j++)
                            {
                                currentIndex++;
                                if (line[j] == Comma)
                                {
                                    break;
                                }
                                if (line[j] != Space)
                                {
                                    throw new FormatException("Invalid field value list:" + line);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (!canSkipSpace)
                        {
                            string errorMsg = $"Single quotation should be escaped. Position index:{currentIndex}, field value list: {line}";
                            throw new FormatException(errorMsg);
                        }
                        escaped = true;
                        startIndex = currentIndex + 1;
                    }
                }
                else if (c == Comma || c == BackQuote)
                {
                    if (!escaped)
                    {
                        endIndex = currentIndex;
                    }
                }
                else if (currentIndex == line.Length - 1)
                {
                    endIndex = currentIndex + 1;
                }

                if (canSkipSpace && c != Space)
                {
                    canSkipSpace = false;
                }

                if (endIndex != null)
                {
                    string word = line.Substring(startIndex, endIndex.Value - startIndex).Trim();
                    if (!escaped && string.Compare(word, "NULL", true) == 0)
                    {
                        // Handle null case, for example: insert into t1(Id,Val) values(1, NULL)
                        columnValues.Add(null);
                    }
                    else
                    {
                        columnValues.Add(word.Replace("\'\'", "\'"));
                    }

                    startIndex = currentIndex + 1;
                    endIndex = null;

                    escaped = false;
                    canSkipSpace = true;

                    if (c == BackQuote)
                    {
                        HashSet<char> skippedChars = new HashSet<char>() { ' ', ')', ',', '(', '\r', '\n' };
                        while (currentIndex < line.Length)
                        {
                            char ch = line[currentIndex];
                            if (skippedChars.Contains(ch))
                            {
                                currentIndex++;
                            }
                            else
                            {
                                break;
                            }
                        }
                        nextIndex = currentIndex;
                        break;
                    }
                }
            }

            if (line.Length == 0 || line[line.Length - 1] == Comma)
            {
                // Add empty string into list: 1)it ends with comma  2)its length is 0
                columnValues.Add("");
            }

            return columnValues;
        }

        private DbClient m_dbClient = null;

        private static readonly char SingleQuotation = '\'';
        private static readonly char Comma = ',';
        private static readonly char BackQuote = ')';
        private static readonly char Space = ' ';

        private readonly static string TableNamePattern = @"(?<TableName>[0-9A-Za-z]+)";
        private readonly static string FieldNamePattern = @"(?<FieldName>[0-9A-Za-z]+)";
        private readonly static string AsNamePattern = @"(?<AsName>[0-9A-Za-z]+)";
        private readonly static string SelectWherePattern = @"(?<Where>.+?)";
        private readonly static string DeleteWherePattern = @"(?<Where>.+)";
        private readonly static string ShowPattern = @"show\s+(?<ShowValue>.+)";
        private readonly static string FileNamePattern = @"(?<FileName>[0-9A-Za-z.-_]+)";
        private readonly static string LogFileNamePattern = @"(?<LogFileName>[0-9A-Za-z.-_]+)";

        private readonly static string TopNPattern = @"top\s+(?<TopN>\d{1,10})";
        private readonly static string LimitPattern = @"limit\s+(?<Limit>\d{1,10})";
        private readonly static string GroupByListPattern = @"(?<GroupByList>[0-9A-Za-z,]{1,300})";
        private readonly static string FieldListPattern = @"(?<FieldList>.+)";
        private readonly static string AggregateType = @"(?<AggregateType>(count)|(avg)|(sum)|(min)|(max))";
        private readonly static string AggregateLine = string.Format(@"{0}\s*\(\s*{1}\s*\)\s+as\s+{2}", AggregateType, FieldNamePattern, AsNamePattern);

        private readonly static string Optional = @"{0,1}";

        private readonly static Regex AggregateLineRegex = new Regex(AggregateLine, RegexOptions.Singleline | RegexOptions.IgnoreCase);

        private readonly static string SelectPattern = string.Format(
            @"^\s*select(\s+{1}){0}\s+((\*)|{6})\s+from\s+{2}\s+where\s+{3}(\s+{4}){0}(\s+group\s+by\s+{5}){0}\s*$",
            Optional, TopNPattern, TableNamePattern, SelectWherePattern, LimitPattern, GroupByListPattern, FieldListPattern);

        private readonly static string InsertPattern = string.Format(@"insert\s+into\s+{0}\s*\(\s*(?<FieldNameList>.+)\s*\)\s+values\s*\((?<FieldValueList>.+)\s*\)",
            TableNamePattern);

        private readonly static string DeletePattern = string.Format(@"delete\s+from\s+{0}\s+where\s+{1}",
            TableNamePattern, DeleteWherePattern);

        private readonly static string DropTablePattern = string.Format(@"drop\s+table\s+{0}", TableNamePattern);

        private readonly static string TruncateTablePattern = string.Format(@"truncate\s+table\s+{0}", TableNamePattern);

        private readonly static string ImportTablePattern = string.Format(@"imp\s+{0}\s+file\s*=\s*{1}", TableNamePattern, FileNamePattern);
        private readonly static string ServerImportTablePattern = string.Format(@"server\s+imp\s+{0}\s+file\s*=\s*{1}\s+logfile\s*=\s*{2}",
            TableNamePattern, FileNamePattern, LogFileNamePattern);

        private readonly static string CreateTablePattern = string.Format(@"create\s+table\s+{0}\s*\(\s*(?<FieldDefList>.+)\s*\)", TableNamePattern);


        private readonly static Regex[] SelectRegexes = {
            new Regex(SelectPattern, RegexOptions.Singleline | RegexOptions.IgnoreCase | RegexOptions.ExplicitCapture)
        };
        private readonly static Regex InsertRegex = new Regex(InsertPattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex DeleteRegex = new Regex(DeletePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex DropTableRegex = new Regex(DropTablePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex TruncateTableRegex = new Regex(TruncateTablePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex ImportTableRegex = new Regex(ImportTablePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex ServerImportTableRegex = new Regex(ServerImportTablePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);

        private readonly static Regex CreateTableRegex = new Regex(CreateTablePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex FieldNameRegex = new Regex("^" + FieldNamePattern + "$", RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex ShowRegex = new Regex("^" + ShowPattern + "$", RegexOptions.Singleline | RegexOptions.IgnoreCase);

    }
}