namespace HDBPublic
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;
    using System.Xml;


    class DbClient
    {
        public readonly string Version = "1.1";

        private string m_hostName;
        private int m_port;
        private RequestClient m_requestClient;

        public event EventHandler<BeforeRequestArgs> BeforeRequest;
        public event EventHandler<AfterResponseArgs> AfterResponse;

        public event EventHandler<RequestProgressArgs> RequestProgress;


        public DbClient(string hostName, int port)
        {
            m_hostName = hostName;
            m_port = port;
            m_requestClient = new RequestClient(hostName, port);
        }

        public void Add(QueryInfo queryInfo)
        {
            RequestData(OpType.Add, queryInfo);
        }

        public void Update(QueryInfo queryInfo)
        {
            RequestData(OpType.Update, queryInfo);
        }

        public void Delete(QueryInfo queryInfo)
        {
            RequestData(OpType.Del, queryInfo);
        }

        public DataTable Query(QueryInfo queryInfo)
        {
            (string outputFieldList, _) = GenerateInvolvedFieldInfo(queryInfo);
            DataTable result = new DataTable();
            List<string> responseResult = RequestData(OpType.Get, queryInfo);
            foreach (string responseText in responseResult)
            {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(responseText);
                XmlNodeList nodeList = doc.SelectNodes("/Result/Data");

                foreach (XmlNode node in nodeList)
                {
                    if (result.Columns.Count == 0)
                    {
                        if (string.IsNullOrWhiteSpace(outputFieldList))
                        {
                            foreach (XmlNode tempNode in node.ChildNodes)
                            {
                                result.Columns.Add(tempNode.Name);
                            }
                        }
                        else
                        {
                            string[] items = outputFieldList.Split(",", StringSplitOptions.RemoveEmptyEntries);
                            foreach (string fieldName in items)
                            {
                                result.Columns.Add(fieldName);
                            }
                        }
                    }

                    DataRow drNew = result.NewRow();
                    foreach (XmlNode tempNode in node.ChildNodes)
                    {
                        XmlNode nilNode = tempNode.SelectSingleNode("@Nil");
                        if (nilNode != null && nilNode.InnerText == "1")
                        {
                            continue;
                        }
                        drNew[tempNode.Name] = tempNode.InnerText;
                    }
                    result.Rows.Add(drNew);
                }
            }

            return result;
        }

        public bool Hi(out string result)
        {
            bool success = true;
            result = "OK";
            try
            {
                SendRequest(RequestType.Hi, null);
            }
            catch (Exception ex)
            {
                success = false;
                result = ex.Message;
            }

            return success;
        }

        public bool Stop(out string result)
        {
            bool success = true;
            result = "OK";
            try
            {
                SendRequest(RequestType.Stop, null);
            }
            catch (Exception ex)
            {
                success = false;
                result = ex.Message;
            }

            return success;
        }

        public DataTable GetTableList()
        {
            // <Msg Op='GetTableList'></Msg>
            string responseText = RequestTableOp(OpType.GetTableList, null);

            /*
            <Result Status="1" StatusText="OK">
                <Table Name="Issuers">
                    <Column Name="Id" DataType="Varchar" PK="1" CompressType="LZ4" />
                    <Column Name="Name" DataType="Varchar" CompressType="LZ4" />
                    <Column Name="Price" DataType="Double" CompressType="LZ4" />
                </Table>
            </Result>
            */
            DataTable result = new DataTable();
            result.Columns.Add("TableName", typeof(string));
            result.Columns.Add("ColumnName", typeof(string));
            result.Columns.Add("DataType", typeof(string));
            result.Columns.Add("PK", typeof(bool));
            result.Columns.Add("CompressType", typeof(string));
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(responseText);
            XmlNodeList tableNodeList = doc.SelectNodes("/Result/Table");
            foreach (XmlNode tableNode in tableNodeList)
            {
                string tableName = XmlHelper.GetNodeText(tableNode, "@Name");
                XmlNodeList columnNodeList = tableNode.SelectNodes("Column");
                foreach (XmlNode columnNode in columnNodeList)
                {
                    string columnName = XmlHelper.GetNodeText(columnNode, "@Name");
                    string dataType = XmlHelper.GetNodeText(columnNode, "@DataType");
                    string pkString = XmlHelper.GetNodeText(columnNode, "@PK");
                    bool pk = pkString == "1";
                    string compressType = XmlHelper.GetNodeText(columnNode, "@CompressType");
                    result.Rows.Add(new object[] { tableName, columnName, dataType, pk, compressType });
                }
            }
            result.AcceptChanges();
            return result;
        }

        public string RemoveTable(string tableName)
        {
            // <Msg Op='RemoveTable' Table='Issuers2'></Msg>
            return RequestTableOp(OpType.RemoveTable, tableName);
        }

        public string ServerImportTable(string tableName, string fileName, string logFileName)
        {
            // <Msg Op='ImportTable' Table='Issuers' File='d:\\a.txt' LogFile='d:\\a.log'></Msg>
            return ImpExpData(OpType.ImportTable, tableName, fileName, logFileName);
        }

        public string TruncateTable(string tableName)
        {
            // <Msg Op='TruncateTable' Table='Issuers2'></Msg>
            return RequestTableOp(OpType.TruncateTable, tableName);
        }

        public void CreateTable(string tableName, List<ColumnDefinition> columnDefinitions)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<Msg></Msg>");
            XmlNode msgNode = doc.SelectSingleNode("/Msg");
            XmlHelper.AddAttribute(msgNode, "Op", "CreateTable");
            XmlHelper.AddAttribute(msgNode, "Table", tableName);

            /*
            <Msg Op="CreateTable" Table="Issuers">
            <Column Name=""SecId"" DataType=""int"" PK=""1""></Column>
            <Column Name=""Name"" DataType=""varchar"" PK=""""></Column>
            </Msg>
            */
            foreach (var columnDefinition in columnDefinitions)
            {
                XmlNode colNode = XmlHelper.AddSubNode(msgNode, "Column", "");
                XmlHelper.SetAttribute(colNode, "Name", columnDefinition.Name);
                XmlHelper.SetAttribute(colNode, "DataType", columnDefinition.DataType.ToString());

                if (columnDefinition.PK)
                {
                    XmlHelper.SetAttribute(colNode, "PK", columnDefinition.PK ? "1" : "0");
                }
                if (!string.IsNullOrWhiteSpace(columnDefinition.CompressType))
                {
                    XmlHelper.SetAttribute(colNode, "CompressType", columnDefinition.CompressType);
                }
            }

            string responseXml = SendRequest(RequestType.Request, doc.OuterXml);
            XmlDocument docResponse = new XmlDocument();
            docResponse.LoadXml(responseXml);
            XmlNode statusNode = docResponse.SelectSingleNode("/Result/@Status");
            if (statusNode == null)
            {
                throw new ApplicationException("Invalid response.");
            }
            if (statusNode.InnerText != "1")
            {
                string statusText = docResponse.SelectSingleNode("/Result/@StatusText").InnerText;
                throw new ApplicationException($"Request failed: {statusText}");
            }
        }

        private string RequestTableOp(OpType op, string tableName)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<Msg></Msg>");
            XmlNode msgNode = doc.SelectSingleNode("/Msg");
            XmlHelper.AddAttribute(msgNode, "Op", op.ToString());
            if (!string.IsNullOrEmpty(tableName))
            {
                XmlHelper.AddAttribute(msgNode, "Table", tableName);
            }

            string responseXml = SendRequest(RequestType.Request, doc.OuterXml);
            XmlDocument docResponse = new XmlDocument();
            docResponse.LoadXml(responseXml);
            XmlNode statusNode = docResponse.SelectSingleNode("/Result/@Status");
            if (statusNode == null)
            {
                throw new ApplicationException("Invalid response.");
            }
            if (statusNode.InnerText != "1")
            {
                string statusText = docResponse.SelectSingleNode("/Result/@StatusText").InnerText;
                throw new ApplicationException($"Request failed: {statusText}");
            }

            return responseXml;
        }

        private string ImpExpData(OpType op, string tableName, string fileName, string logFileName)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<Msg></Msg>");
            XmlNode msgNode = doc.SelectSingleNode("/Msg");
            XmlHelper.AddAttribute(msgNode, "Op", op.ToString());
            if (!string.IsNullOrEmpty(tableName))
            {
                XmlHelper.AddAttribute(msgNode, "Table", tableName);
            }
            XmlHelper.AddAttribute(msgNode, "File", fileName);
            XmlHelper.AddAttribute(msgNode, "LogFile", logFileName);

            string responseXml = SendRequest(RequestType.Request, doc.OuterXml, true);
            if (!string.IsNullOrEmpty(responseXml))
            {
                XmlDocument docResponse = new XmlDocument();
                docResponse.LoadXml(responseXml);
                XmlNode statusNode = docResponse.SelectSingleNode("/Result/@Status");
                if (statusNode == null)
                {
                    throw new ApplicationException("Invalid response.");
                }
                if (statusNode.InnerText != "1")
                {
                    string statusText = docResponse.SelectSingleNode("/Result/@StatusText").InnerText;
                    throw new ApplicationException($"Request failed: {statusText}");
                }
            }
            return "";
        }

        public List<string> RequestData(OpType op, QueryInfo queryInfo)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<Msg></Msg>");
            XmlNode msgNode = doc.SelectSingleNode("/Msg");
            XmlHelper.AddAttribute(msgNode, "Op", op.ToString());
            if (!string.IsNullOrEmpty(queryInfo.TableName))
            {
                XmlHelper.AddAttribute(msgNode, "Table", queryInfo.TableName);
            }
            if (queryInfo.Limit != null)
            {
                XmlHelper.AddAttribute(msgNode, "Limit", queryInfo.Limit.Value.ToString("0"));
            }

            (string outputFieldList, string involvedFieldList) = GenerateInvolvedFieldInfo(queryInfo);

            List<string> result = new List<string>();
            const int ProgressCount = 10000;
            const int MaxBatchCount = 2000;
            int currentCount = 0;
            int totalCount = queryInfo.FieldConditions.Count;
            foreach (Dictionary<string, Tuple<Object, PredicateType>> fieldValueMap in queryInfo.FieldConditions)
            {
                currentCount++;
                if (fieldValueMap != null)
                {
                    XmlNode dataNode = doc.CreateElement("Data");
                    msgNode.AppendChild(dataNode);
                    foreach (string key in fieldValueMap.Keys)
                    {
                        string val = ConvertValueToString(fieldValueMap[key].Item1);
                        if (fieldValueMap[key].Item2 == PredicateType.EQ)
                        {
                            XmlHelper.AddSubNode(dataNode, key, val);
                        }
                        else
                        {
                            XmlHelper.AddSubNodeWithPredicateType(dataNode, key, val, fieldValueMap[key].Item2);
                        }
                    }
                }

                if (currentCount % MaxBatchCount == 0 || currentCount == totalCount)
                {
                    AttachInvolvedFieldInfo(doc, outputFieldList, involvedFieldList);
                    AttachGroupBy(doc, queryInfo);
                    string responseXml = SendRequest(RequestType.Request, doc.OuterXml);
                    XmlDocument docResponse = new XmlDocument();
                    docResponse.LoadXml(responseXml);
                    XmlNode statusNode = docResponse.SelectSingleNode("/Result/@Status");
                    if (statusNode == null)
                    {
                        throw new ApplicationException("Invalid response.");
                    }
                    if (statusNode.InnerText != "1")
                    {
                        string statusText = docResponse.SelectSingleNode("/Result/@StatusText").InnerText;
                        throw new ApplicationException($"Request failed: {statusText}");
                    }

                    if (currentCount % ProgressCount == 0)
                    {
                        RequestProgress?.Invoke(null, new RequestProgressArgs(totalCount, currentCount));
                    }

                    doc = new XmlDocument();
                    doc.LoadXml("<Msg></Msg>");
                    msgNode = doc.SelectSingleNode("/Msg");
                    XmlHelper.AddAttribute(msgNode, "Op", OpType.Add.ToString());
                    if (!string.IsNullOrEmpty(queryInfo.TableName))
                    {
                        XmlHelper.AddAttribute(msgNode, "Table", queryInfo.TableName);
                    }

                    result.Add(responseXml);
                }
            }
            return result;
        }

        private void AttachInvolvedFieldInfo(XmlDocument doc, string outputFieldList, string involvedFieldList)
        {
            /*
            <Msg Op='Get' Table='Issuers' Limit='2' InvolvedFields='Id,Price' OutputFields='Price'>
                <Data><Id>S001</Id></Data>
            </Msg>
            */
            XmlNode msgNode = doc.SelectSingleNode("/Msg");
            if (msgNode == null)
            {
                return;
            }

            if (outputFieldList != null)
            {
                XmlHelper.AddAttribute(msgNode, "OutputFields", outputFieldList);
            }
            if (involvedFieldList != null)
            {
                XmlHelper.AddAttribute(msgNode, "InvolvedFields", involvedFieldList);
            }
        }

        private (string outputFieldList, string involvedFieldList) GenerateInvolvedFieldInfo(QueryInfo queryInfo)
        {
            bool hasSelectList = queryInfo.RawFieldInfos?.Count > 0 || queryInfo.AggregateInfos?.Count > 0;
            if (!hasSelectList)
            {
                return (null, null);
            }

            StringBuilder outputFieldBuilder = new StringBuilder();
            foreach (var outputFieldName in queryInfo.OutputFields)
            {
                if (outputFieldBuilder.Length > 0)
                {
                    outputFieldBuilder.Append(",");
                }
                outputFieldBuilder.Append(outputFieldName);
            }

            HashSet<string> involvedFields = new HashSet<string>();
            foreach (var fieldInfo in queryInfo.RawFieldInfos)
            {
                if (!involvedFields.Contains(fieldInfo.FieldName))
                {
                    involvedFields.Add(fieldInfo.FieldName);
                }
            }
            foreach (var aggregateInfo in queryInfo.AggregateInfos)
            {
                if (!involvedFields.Contains(aggregateInfo.FieldName))
                {
                    involvedFields.Add(aggregateInfo.FieldName);
                }
            }
            foreach (var fieldCondition in queryInfo.FieldConditions)
            {
                foreach (string fieldName in fieldCondition.Keys)
                {
                    if (!involvedFields.Contains(fieldName))
                    {
                        involvedFields.Add(fieldName);
                    }
                }
            }
            foreach (var groupByFieldName in queryInfo.GroupBys)
            {
                if (!involvedFields.Contains(groupByFieldName))
                {
                    involvedFields.Add(groupByFieldName);
                }
            }
            return (outputFieldBuilder.ToString(), string.Join(",", involvedFields));
        }

        private void AttachGroupBy(XmlDocument doc, QueryInfo queryInfo)
        {
            /*
            <Group By='Name'>
                <Column Name='Id' As='IdCount' AggregateType='count'></Column>
                <Column Name='Price' As='PriceAvg' AggregateType='avg'></Column>
                <Column Name='Price' As='PriceSum' AggregateType='sum'></Column>
            </Group>
            */
            if (queryInfo.AggregateInfos?.Count > 0 && queryInfo.GroupBys?.Length > 0)
            {
                XmlNode msgNode = doc.SelectSingleNode("/Msg");
                if (msgNode != null)
                {
                    var groupNode = doc.CreateElement("Group");
                    XmlHelper.AddAttribute(groupNode, "By", string.Join(",", queryInfo.GroupBys));
                    foreach (var aggregateInfo in queryInfo.AggregateInfos)
                    {
                        var columnNode = doc.CreateElement("Column");
                        XmlHelper.AddAttribute(columnNode, "Name", aggregateInfo.FieldName);
                        XmlHelper.AddAttribute(columnNode, "As", aggregateInfo.AsName);
                        XmlHelper.AddAttribute(columnNode, "AggregateType", aggregateInfo.AggregateType);

                        groupNode.AppendChild(columnNode);
                    }

                    msgNode.AppendChild(groupNode);
                }
            }
        }

        private string SendRequest(RequestType requestType, string requestText, bool noNeedReturn = false)
        {
            if (requestType == RequestType.Request && string.IsNullOrEmpty(requestText))
            {
                throw new ArgumentException("Message cannot be empty.");
            }

            BeforeRequest?.Invoke(null, new BeforeRequestArgs(requestText));
            string responseText = m_requestClient.Call(requestType, requestText, noNeedReturn);
            AfterResponse?.Invoke(null, new AfterResponseArgs(requestText, responseText));
            return responseText;
        }

        private static string ConvertValueToString(object obj)
        {
            if (obj == null) return null;

            string val = null;
            if (obj is DateTime)
            {
                val = Convert.ToDateTime(obj).ToString("yyyyMMdd");
            }
            else if (obj is float || obj is double || obj is decimal)
            {
                val = Convert.ToDouble(obj).ToString("0.##########");
            }
            else if (obj is byte || obj is Int16 || obj is Int32)
            {
                val = Convert.ToInt32(obj).ToString();
            }
            else if (obj is long || obj is Int64)
            {
                val = Convert.ToInt64(obj).ToString();
            }
            else if (obj is string)
            {
                val = obj.ToString();
            }
            else
            {
                throw new NotSupportedException($"Cannot support this data type: {obj.GetType().FullName}");
            }

            return val;
        }

    }

    record BeforeRequestArgs(string RequestText);

    record AfterResponseArgs(string RequestText, string ResponseText);

    record RequestProgressArgs(int TotalCount, int CompleteCount);

}
