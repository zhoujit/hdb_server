namespace HDBPublic
{
    using System;
    using System.Collections.Generic;
    using System.Data;
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

        public void Add(string tableName, List<Dictionary<string, object>> fieldConditions)
        {
            RequestData(OpType.Add, tableName, fieldConditions);
        }

        public void Update(string tableName, List<Dictionary<string, object>> fieldConditions)
        {
            RequestData(OpType.Update, tableName, fieldConditions);
        }

        public void Delete(string tableName, List<Dictionary<string, object>> fieldConditions)
        {
            RequestData(OpType.Del, tableName, fieldConditions);
        }

        public DataTable Query(string tableName, List<Dictionary<string, object>> fieldConditions)
        {
            DataTable result = new DataTable();
            List<string> responseResult = RequestData(OpType.Get, tableName, fieldConditions);
            foreach (string responseText in responseResult)
            {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(responseText);
                XmlNodeList nodeList = doc.SelectNodes("/Result/Data");

                foreach (XmlNode node in nodeList)
                {
                    if (result.Columns.Count == 0)
                    {
                        foreach (XmlNode tempNode in node.ChildNodes)
                        {
                            result.Columns.Add(tempNode.Name);
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
            string responseText = RequestData(OpType.GetTableList, null);

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
            return RequestData(OpType.RemoveTable, tableName);
        }

        public string TruncateTable(string tableName)
        {
            // <Msg Op='TruncateTable' Table='Issuers2'></Msg>
            return RequestData(OpType.TruncateTable, tableName);
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
                throw new ApplicationException(string.Format("Request failed: {0}",
                    docResponse.SelectSingleNode("/Result/@StatusText").InnerText));
            }
        }

        private string RequestData(OpType op, string tableName)
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
                throw new ApplicationException(string.Format("Request failed: {0}",
                    docResponse.SelectSingleNode("/Result/@StatusText").InnerText));
            }

            return responseXml;
        }

        public List<string> RequestData(OpType op, string tableName, List<Dictionary<string, object>> fieldConditions)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml("<Msg></Msg>");
            XmlNode msgNode = doc.SelectSingleNode("/Msg");
            XmlHelper.AddAttribute(msgNode, "Op", op.ToString());
            if (!string.IsNullOrEmpty(tableName))
            {
                XmlHelper.AddAttribute(msgNode, "Table", tableName);
            }

            List<string> result = new List<string>();
            const int ProgressCount = 10000;
            const int MaxBatchCount = 2000;
            int currentCount = 0;
            int totalCount = fieldConditions.Count;
            foreach (Dictionary<string, object> fieldValueMap in fieldConditions)
            {
                currentCount++;
                if (fieldValueMap != null)
                {
                    XmlNode dataNode = doc.CreateElement("Data");
                    msgNode.AppendChild(dataNode);
                    foreach (string key in fieldValueMap.Keys)
                    {
                        string val = ConvertValueToString(fieldValueMap[key]);
                        XmlHelper.AddSubNode(dataNode, key, val);
                    }
                }

                if (currentCount % MaxBatchCount == 0 || currentCount == totalCount)
                {
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
                        throw new ApplicationException(string.Format("Request failed: {0}",
                            docResponse.SelectSingleNode("/Result/@StatusText").InnerText));
                    }

                    if (currentCount % ProgressCount == 0)
                    {
                        RequestProgress?.Invoke(null, new RequestProgressArgs(totalCount, currentCount));
                    }

                    doc = new XmlDocument();
                    doc.LoadXml("<Msg></Msg>");
                    msgNode = doc.SelectSingleNode("/Msg");
                    XmlHelper.AddAttribute(msgNode, "Op", OpType.Add.ToString());
                    if (!string.IsNullOrEmpty(tableName))
                    {
                        XmlHelper.AddAttribute(msgNode, "Table", tableName);
                    }

                    result.Add(responseXml);
                }
            }
            return result;
        }

        private string SendRequest(RequestType requestType, string requestText)
        {
            if (requestType == RequestType.Request && string.IsNullOrEmpty(requestText))
            {
                throw new ArgumentException("Message cannot be empty.");
            }

            BeforeRequest?.Invoke(null, new BeforeRequestArgs(requestText));
            string responseText = m_requestClient.Call(requestType, requestText);
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
                val = Convert.ToDouble(obj).ToString("0.#####");
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
                throw new NotSupportedException(string.Format("Cannot support this data type: {0}", obj.GetType().FullName));
            }

            return val;
        }

    }

    class BeforeRequestArgs : EventArgs
    {
        public readonly string RequestText;

        public BeforeRequestArgs(string requestText)
        {
            this.RequestText = requestText;
        }
    }

    class AfterResponseArgs : EventArgs
    {
        public readonly string RequestText;
        public readonly string ResponseText;

        public AfterResponseArgs(string requestText, string responseText)
        {
            this.RequestText = requestText;
            this.ResponseText = responseText;
        }
    }

    class RequestProgressArgs : EventArgs
    {
        public readonly int TotalCount;
        public readonly int CompleteCount;

        public RequestProgressArgs(int totalCount, int completeCount)
        {
            this.TotalCount = totalCount;
            this.CompleteCount = completeCount;
        }
    }

}
