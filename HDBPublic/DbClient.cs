using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

namespace HDBPublic
{
    public class DbClient
    {
        public readonly string Version = "1.1";

        private string m_hostName;
        private int m_port;
        private RequestClient m_requestClient;

        public static event EventHandler<BeforeRequestArgs> BeforeRequest;
        public static event EventHandler<AfterResponseArgs> AfterResponse;

        public event EventHandler<BatchRequestProgressArgs> BatchRequestProgress;


        public DbClient(string hostName, int port)
        {
            m_hostName = hostName;
            m_port = port;
            m_requestClient = new RequestClient(hostName, port);
        }

        public void Add(string tableName, List<Dictionary<string, object>> fieldValues)
        {
            RequestData(OpType.Add, tableName, fieldValues);
        }

        public void Update(string tableName, List<Dictionary<string, object>> fieldValues)
        {
            RequestData(OpType.Update, tableName, fieldValues);
        }

        public void Delete(string tableName, List<Dictionary<string, object>> fieldValues)
        {
            RequestData(OpType.Del, tableName, fieldValues);
        }

        public DataTable Query(string tableName, List<Dictionary<string, object>> fieldValues)
        {
            DataTable dtResult = new DataTable();
            List<string> responseResult = RequestData(OpType.Get, tableName, fieldValues);
            foreach (string responseText in responseResult)
            {
                XmlDocument doc = new XmlDocument();
                doc.LoadXml(responseText);
                XmlNodeList nodeList = doc.SelectNodes("/Result/Data");

                foreach (XmlNode node in nodeList)
                {
                    if (dtResult.Columns.Count == 0)
                    {
                        foreach (XmlNode tempNode in node.ChildNodes)
                        {
                            dtResult.Columns.Add(tempNode.Name);
                        }
                    }

                    DataRow drNew = dtResult.NewRow();
                    foreach (XmlNode tempNode in node.ChildNodes)
                    {
                        XmlNode nilNode = tempNode.SelectSingleNode("@Nil");
                        if (nilNode != null && nilNode.InnerText == "1")
                        {
                            continue;
                        }
                        drNew[tempNode.Name] = tempNode.InnerText;
                    }
                    dtResult.Rows.Add(drNew);
                }
            }

            return dtResult;
        }

        public bool Hi(out string result)
        {
            bool success = true;
            result = "OK";
            try
            {
                m_requestClient.Hi();
            }
            catch (Exception ex)
            {
                success = false;
                result = ex.Message;
            }

            return success;
        }

        public string GetTableList()
        {
            // <Msg Op='GetTableList'></Msg>
            return RequestData(OpType.GetTableList, null);
        }

        public string RemoveTable(string tableName)
        {
            // <Msg Op='RemoveTable' Table='Issuers2'></Msg>
            return RequestData(OpType.RemoveTable, tableName);
        }

        public void CreateTable(string tableName, DataTable dt)
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
            foreach (DataRow dr in dt.Rows)
            {
                XmlNode colNode = XmlHelper.AddSubNode(msgNode, "Column", "");
                XmlHelper.SetAttribute(colNode, "Name", dr["Name"].ToString());
                XmlHelper.SetAttribute(colNode, "DataType", dr["DataType"].ToString());

                if (!dr.IsNull("PK"))
                {
                    XmlHelper.SetAttribute(colNode, "PK", Convert.ToBoolean(dr["PK"]) ? "1" : "0");
                }
            }

            string responseXml = SendMsg(doc);
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

            string responseXml = SendMsg(doc);
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

        public List<string> RequestData(OpType op, string tableName, List<Dictionary<string, object>> fieldValues)
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
            int totalCount = fieldValues.Count;
            foreach (Dictionary<string, object> fieldValueMap in fieldValues)
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
                    string responseXml = SendMsg(doc);
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
                        if (BatchRequestProgress != null)
                        {
                            BatchRequestProgress(null, new BatchRequestProgressArgs(totalCount, currentCount));
                        }
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

        private string SendMsg(XmlDocument msg)
        {
            if (msg.OuterXml.Length <= 0)
            {
                throw new ArgumentException("Message cannot be empty.");
            }

            byte[] arrBody = Encoding.UTF8.GetBytes(msg.OuterXml);
            string header = string.Format("HDB:{0}\nLength:{1}\n\n", Version, arrBody.Length);
            byte[] arrHeader = Encoding.UTF8.GetBytes(header);

            if (BeforeRequest != null)
            {
                BeforeRequest(null, new BeforeRequestArgs(header + msg.OuterXml));
            }

            string responseText = null;
            using (System.Net.Sockets.TcpClient client = new System.Net.Sockets.TcpClient(m_hostName, m_port))
            {
                using (NetworkStream stream = client.GetStream())
                {
                    stream.Write(arrHeader, 0, arrHeader.Length);
                    stream.Write(arrBody, 0, arrBody.Length);
                    stream.Flush();

                    // Cache all bytes to prevent splitted UTF8 bytes
                    using (MemoryStream ms = new MemoryStream())
                    {
                        StringBuilder result = new StringBuilder();
                        const int MaxBuffSize = 1024;
                        byte[] buff = new byte[MaxBuffSize];
                        int bodyLength = -1;
                        int currentBodyPos = -1;
                        Regex regex = new Regex("Length:(?<len>\\d+)\\n", RegexOptions.Singleline);

                        Stopwatch stepTime = new Stopwatch();
                        stepTime.Start();
                        while (true)
                        {
                            if (stepTime.Elapsed.TotalSeconds > 30)
                            {
                                throw new TimeoutException("Receive timeout.");
                            }
                            int byteCount = stream.Read(buff, 0, MaxBuffSize);
                            if (byteCount > 0)
                            {
                                result.Append(Encoding.UTF8.GetString(buff, 0, byteCount));
                                ms.Write(buff, 0, byteCount);
                            }

                            string currentResult = result.ToString();
                            int headerEndPos = currentResult.IndexOf("\n\n");
                            if (headerEndPos > 0)
                            {
                                Match match = regex.Match(currentResult);
                                if (match.Success)
                                {
                                    bodyLength = int.Parse(match.Groups["len"].Value);
                                }

                                if (bodyLength < 0)
                                {
                                    throw new ApplicationException("Invalid response.");
                                }

                                // Position base on byte level
                                // So the every character for header must be 0~127
                                // -2: exclude 2 enter characters: \n\n
                                currentBodyPos = (int)ms.Length - headerEndPos - 2;
                                break;
                            }

                            if (ms.Length > 512) // 512: current allowed max header length
                            {
                                throw new ApplicationException("Invalid response.");
                            }
                        }

                        while (currentBodyPos < bodyLength)
                        {
                            int byteCount = stream.Read(buff, 0, MaxBuffSize);
                            if (byteCount > 0)
                            {
                                currentBodyPos += byteCount;
                                ms.Write(buff, 0, byteCount);
                            }
                        }

                        ms.Position = 0;
                        StreamReader reader = new StreamReader(ms, Encoding.UTF8);
                        responseText = reader.ReadToEnd();
                    }

                }
            }

            if (AfterResponse != null)
            {
                AfterResponse(null, new AfterResponseArgs(header + msg.OuterXml, responseText));
            }

            int tempPos = responseText.IndexOf("\n\n");
            return responseText.Substring(tempPos + 2);
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

    public class BeforeRequestArgs : EventArgs
    {
        public readonly string RequestText;

        public BeforeRequestArgs(string requestText)
        {
            this.RequestText = requestText;
        }
    }

    public class AfterResponseArgs : EventArgs
    {
        public readonly string RequestText;
        public readonly string ResponseText;

        public AfterResponseArgs(string requestText, string responseText)
        {
            this.RequestText = requestText;
            this.ResponseText = responseText;
        }
    }

    public class BatchRequestProgressArgs : EventArgs
    {
        public readonly int TotalCount;
        public readonly int CompleteCount;

        public BatchRequestProgressArgs(int totalCount, int completeCount)
        {
            this.TotalCount = totalCount;
            this.CompleteCount = completeCount;
        }
    }

}
