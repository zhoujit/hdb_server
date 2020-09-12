using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.RegularExpressions;

namespace HDBPublic
{
    public class SQLStatement
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
                Match match = SelectRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string whereStatment = match.Groups["Where"].Value.Trim();

                    List<Dictionary<string, object>> dctField = ParseWhereStatement(whereStatment);

                    stepTime.Start();
                    result = m_dbClient.Query(tableName, dctField);
                    stepTime.Stop();

                    message = string.Format("Returned {0} record(s). Elapsed:{1}s\n", result.Rows.Count, stepTime.Elapsed.TotalSeconds.ToString("0.###"));
                }
                else
                {
                    message = string.Format(@"Invalid select statement:{0}
Memo:
1. Select statement must contains where clause, and field list must be *
2. Two consecutive single-quotes in string, which are treated as a literal single-quote

Example:
select * from t1 where f1 = 100;
", sql);
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

                    List<Dictionary<string, object>> dctValue = new List<Dictionary<string, object>>();
                    success = FillFieldValue(fieldNameList, fieldValueList, dctValue, out message);
                    if (success)
                    {
                        stepTime.Start();
                        m_dbClient.Add(tableName, dctValue);
                        stepTime.Stop();

                        message = string.Format("Succeed to append record(s). Elapsed:{0}s", stepTime.Elapsed.TotalSeconds.ToString("0.###"));
                    }
                }
                else
                {
                    message = string.Format(@"Invalid select statement:{0}", sql);
                }
            }
            else if (sql.StartsWith("delete", StringComparison.CurrentCultureIgnoreCase))
            {
                Match match = DeleteRegex.Match(sql);
                success = match.Success;
                if (success)
                {
                    string tableName = match.Groups["TableName"].Value.Trim();
                    string whereStatment = match.Groups["Where"].Value.Trim();

                    List<Dictionary<string, object>> dctField = ParseWhereStatement(whereStatment);

                    stepTime.Start();
                    m_dbClient.Delete(tableName, dctField);
                    stepTime.Stop();

                    message = string.Format("Succeed to remove records. Elapsed:{0}s", stepTime.Elapsed.TotalSeconds.ToString("0.###"));
                }
                else
                {
                    message = string.Format(@"Invalid delete statement:{0}", sql);
                }
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

        private List<Dictionary<string, object>> ParseWhereStatement(string whereStatment)
        {
            List<Dictionary<string, object>> result = new List<Dictionary<string, object>>();
            whereStatment = whereStatment + " ";
            List<char> lstSplitterChar = new List<char>() { ' ', '\t', '\r', '\n' };
            Dictionary<string, object> dctField = new Dictionary<string, object>();
            bool isName = true;
            int? startIndex = null;
            int? endIndex = null;
            string lastFieldName = null;
            for (int i = 0; i < whereStatment.Length; i++)
            {
                char c = whereStatment[i];
                if (isName)
                {
                    // Field name.
                    if (startIndex == null)
                    {
                        if (!lstSplitterChar.Contains(c))
                        {
                            startIndex = i;
                            lastFieldName = null;
                        }

                        if (c == '=')
                        {
                            throw new Exception(string.Format("Field name cannot be empty."));
                        }
                        continue;
                    }

                    if (lstSplitterChar.Contains(c) || c == '=')
                    {
                        endIndex = i;

                        lastFieldName = whereStatment.Substring(startIndex.Value, endIndex.Value - startIndex.Value).Trim();

                        startIndex = null;
                        endIndex = null;
                        isName = false;

                        while (i < whereStatment.Length)
                        {
                            if (whereStatment[i] == '=')
                            {
                                break;
                            }
                            i++;
                        }
                    }
                }
                else
                {
                    // Field value.
                    if (startIndex == null)
                    {
                        if (!lstSplitterChar.Contains(c))
                        {
                            startIndex = i;
                        }
                        continue;
                    }

                    if ((lstSplitterChar.Contains(c) && whereStatment[startIndex.Value] != SingleQuotation) || c == SingleQuotation)
                    {
                        if (c == SingleQuotation)
                        {
                            if (whereStatment[startIndex.Value] != SingleQuotation)
                            {
                                throw new Exception("String must in single quotation.");
                            }

                            if (i < whereStatment.Length - 1 && whereStatment[i + 1] == SingleQuotation)
                            {
                                // The first single-quotation in doule single-quotation.
                                continue;
                            }

                            int quotationCount = 0;
                            int tempIdx = i;
                            while (tempIdx > startIndex)
                            {
                                if (whereStatment[tempIdx] == SingleQuotation)
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

                        string fieldValue = whereStatment.Substring(startIndex.Value, endIndex.Value - startIndex.Value).Trim();
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
                        dctField.Add(lastFieldName, fieldValue.Replace("\'\'", "\'"));

                        startIndex = null;
                        endIndex = null;
                        isName = true;
                        lastFieldName = null;

                        while (i < whereStatment.Length)
                        {
                            if (lstSplitterChar.Contains(whereStatment[i]))
                            {
                                i++;
                                continue;
                            }
                            break;
                        }
                        if (i == whereStatment.Length)
                        {
                            break;
                        }

                        if (i < whereStatment.Length - 3 && string.Compare("AND", whereStatment.Substring(i, 3), true) != 0)
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

            result.Add(dctField);
            return result;
        }

        private bool FillFieldValue(string fieldNameList, string fieldValueList, List<Dictionary<string, object>> valueSet, out string errorMessage)
        {
            bool result = true;
            errorMessage = "";
            Dictionary<string, object> dctValue = new Dictionary<string, object>();

            string[] fieldNames = fieldNameList.Split(new char[] { ',' });
            string[] fieldValues = ParseFieldValueList(fieldValueList).ToArray();

            if (result && fieldNames.Length != fieldValues.Length)
            {
                errorMessage = string.Format("Mismatched between field name and value list.\n{0}\n{1}", fieldNameList, fieldValueList);
                result = false;
            }

            if (result)
            {
                for (int i = 0; i < fieldNames.Length; i++)
                {
                    if (fieldNames[i].Trim().Length == 0)
                    {
                        errorMessage = "Field name cannot be empty.\n" + fieldNameList;
                        result = false;
                        break;
                    }
                }
            }

            if (result)
            {
                for (int i = 0; i < fieldNames.Length; i++)
                {
                    dctValue.Add(fieldNames[i].Trim(), fieldValues[i]);
                }
            }

            if (dctValue.Count > 0)
            {
                valueSet.Add(dctValue);
            }
            return result;
        }

        public static List<string> ParseFieldValueList(string line)
        {
            List<string> lstColumn = new List<string>();

            bool escaped = false;
            int startIndex = 0;
            int? endIndex = null;
            bool canSkipSpace = true;  // only for prefix space
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == SingleQuotation)
                {
                    if (escaped)
                    {
                        if (i == line.Length - 1)
                        {
                            // last quotation mark
                            endIndex = i;
                        }
                        else if (line[i + 1] == SingleQuotation)
                        {
                            // '' -> '
                            i++;
                            continue;
                        }
                        else
                        {
                            endIndex = i;
                            for (int j = i + 1; j < line.Length; j++)
                            {
                                i++;
                                if (line[j] == Comma)
                                {
                                    break;
                                }
                                if (line[j] != Space)
                                {
                                    throw new FormatException("Invalid field value list" + line);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (!canSkipSpace)
                        {
                            string errorMsg = string.Format("Single quotation should be escaped. Column no:{0}, field value list: {1}", i, line);
                            throw new FormatException(errorMsg);
                        }
                        escaped = true;
                        startIndex = i + 1;
                    }
                }
                else if (c == Comma)
                {
                    if (!escaped)
                    {
                        endIndex = i;
                    }
                }
                else if (i == line.Length - 1)
                {
                    endIndex = i + 1;
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
                        lstColumn.Add(null);
                    }
                    else
                    {
                        lstColumn.Add(word.Replace("\'\'", "\'"));
                    }

                    startIndex = i + 1;
                    endIndex = null;

                    escaped = false;
                    canSkipSpace = true;
                }
            }

            if (line.Length == 0 || line[line.Length - 1] == Comma)
            {
                // Add empty string into list: 1)it ends with comma  2)its length is 0
                lstColumn.Add("");
            }

            return lstColumn;
        }

        private DbClient m_dbClient = null;

        private static readonly char SingleQuotation = '\'';
        private static readonly char Comma = ',';
        private static readonly char Space = ' ';

        private readonly static string TableNamePattern = @"(?<TableName>[0-9A-Za-z]+)";
        private readonly static string FieldNamePattern = @"(?<FieldName>[0-9A-Za-z]+)";
        private readonly static string WherePattern = @"(?<Where>.+)";
        private readonly static string FieldValuePattern = @"(?<FieldValue>.+)";

        private readonly static string SelectPattern = string.Format(@"select\s+\*\s+from\s+{0}\s+where\s+{1}",
            TableNamePattern, WherePattern);

        private readonly static string InsertPattern = string.Format(@"insert\s+into\s+{0}\s*\(\s*(?<FieldNameList>.+)\s*\)\s+values\s*\((?<FieldValueList>.+)\s*\)"
            , TableNamePattern);

        private readonly static string DeletePattern = string.Format(@"delete\s+from\s+{0}\s+where\s+{1}",
            TableNamePattern, WherePattern);

        private readonly static Regex SelectRegex = new Regex(SelectPattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex InsertRegex = new Regex(InsertPattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);
        private readonly static Regex DeleteRegex = new Regex(DeletePattern, RegexOptions.Singleline | RegexOptions.IgnoreCase);

    }
}