namespace HDBPublic
{
    using System.Net;
    using System.IO;
    using System.Text;
    using System;
    using System.Collections.Generic;

    public enum RequestType
    {
        Request = 0,
        Hi = 1,
        Stop = 2,
    }

    enum RequestMethod
    {
        GET = 1,
        Post = 2,
    }

    public class RequestClient
    {
        private string m_hostName;
        private int m_port;

        private Dictionary<RequestType, (RequestMethod method, string req)> m_requestMap;

        public RequestClient(string hostName, int port)
        {
            m_hostName = hostName;
            m_port = port;

            m_requestMap = new Dictionary<RequestType, (RequestMethod method, string req)>
            {
                [RequestType.Hi] = (RequestMethod.GET, "hi"),
                [RequestType.Stop] = (RequestMethod.GET, "stop"),
                [RequestType.Request] = (RequestMethod.Post, "req")
            };
        }

        public string Call(RequestType requestType, string body)
        {
            var requestInfo = m_requestMap[requestType];
            string url = string.Format($"http://{m_hostName}:{m_port}/{requestInfo.req}");
            HttpWebRequest webReq = (HttpWebRequest)WebRequest.Create(new Uri(url));
            webReq.Method = requestInfo.method.ToString();

            if (body != null && requestInfo.method == RequestMethod.Post)
            {
                byte[] byteArray = Encoding.Default.GetBytes(body);
                webReq.ContentLength = byteArray.Length;
                using (Stream reqStream = webReq.GetRequestStream())
                {
                    reqStream.Write(byteArray, 0, byteArray.Length);
                }
            }

            using (HttpWebResponse response = (HttpWebResponse)webReq.GetResponse())
            {
                using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                {
                    return reader.ReadToEnd();
                }
            }
        }

    }
}
