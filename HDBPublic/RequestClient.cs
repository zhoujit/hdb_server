namespace HDBPublic
{
    using System.Net;
    using System.IO;
    using System.Text;
    using System;
    using System.Collections.Generic;

    enum RequestType
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

    class RequestClient
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
            HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(new Uri(url));
            webRequest.Method = requestInfo.method.ToString();

            if (body != null && requestInfo.method == RequestMethod.Post)
            {
                byte[] bytes = Encoding.Default.GetBytes(body);
                webRequest.ContentLength = bytes.Length;
                using (Stream reqStream = webRequest.GetRequestStream())
                {
                    reqStream.Write(bytes, 0, bytes.Length);
                }
            }

            using (HttpWebResponse webResponsee = (HttpWebResponse)webRequest.GetResponse())
            {
                using (StreamReader reader = new StreamReader(webResponsee.GetResponseStream(), Encoding.UTF8))
                {
                    return reader.ReadToEnd();
                }
            }
        }

    }
}
