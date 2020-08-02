namespace HDBPublic
{
    using System.Net;
    using System.IO;
    using System.Xml;
    using System.Text;
    using System;

    public class RequestClient
    {
        private string m_hostName;
        private int m_port;

        public RequestClient(string hostName, int port)
        {
            m_hostName = hostName;
            m_port = port;
        }

        public bool Hi()
        {
            // <Result Status="1" StatusText="HDB Server"></Result>
            string url = string.Format($"http://{m_hostName}:{m_port}/hi");
            string responseText = Get(url);
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(responseText);
            return doc.SelectSingleNode("/Result[@Status='1']") != null;
        }

        public string Get(string url)
        {
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(url);
            {
                request.Method = "GET";
                using (WebResponse response = request.GetResponse())
                {
                    using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                    {
                        return reader.ReadToEnd();
                    }
                }
            }
        }

        public string Post(string url, string body)
        {
            byte[] byteArray = Encoding.Default.GetBytes(body);
            HttpWebRequest webReq = (HttpWebRequest)WebRequest.Create(new Uri(url));
            webReq.Method = "POST";
            webReq.ContentType = "application/x-www-form-urlencoded";
            webReq.ContentLength = byteArray.Length;
            using (Stream reqStream = webReq.GetRequestStream())
            {
                reqStream.Write(byteArray, 0, byteArray.Length);
            }
            using (HttpWebResponse response = (HttpWebResponse)webReq.GetResponse())
            {
                using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.Default))
                {
                    return reader.ReadToEnd();
                }
            }
        }



    }
}
