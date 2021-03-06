namespace HDBPublic
{
    using System.Xml;

    class XmlHelper
    {
        public static void AddAttribute(XmlNode node, string name, string val)
        {
            XmlAttribute temp = node.OwnerDocument.CreateAttribute(name);
            temp.InnerText = val;
            node.Attributes.Append(temp);
        }

        public static XmlNode AddSubNode(XmlNode node, string name, string val)
        {
            XmlNode elem = node.OwnerDocument.CreateElement(name);
            if (val == null)
            {
                SetAttribute(elem, "Nil", "1");
            }
            else
            {
                elem.InnerText = val;
            }
            node.AppendChild(elem);
            return elem;
        }

        public static XmlNode AddSubNodeWithPredicateType(XmlNode node, string name, string val, PredicateType predicateType)
        {
            XmlNode elem = node.OwnerDocument.CreateElement(name);
            if (val == null)
            {
                SetAttribute(elem, "Nil", "1");
            }
            else
            {
                elem.InnerText = val;
            }
            SetAttribute(elem, "Op", predicateType.ToString());
            node.AppendChild(elem);
            return elem;
        }

        public static void SetAttribute(XmlNode node, string name, string val)
        {
            XmlAttribute temp = node.Attributes[name];
            if (temp == null)
            {
                temp = node.OwnerDocument.CreateAttribute(name);
                node.Attributes.Append(temp);
            }
            temp.InnerText = val;
        }

        public static XmlNode SetSubNode(XmlNode node, string name, string val)
        {
            XmlNode elem = node.SelectSingleNode(name);
            if (elem == null)
            {
                elem = node.OwnerDocument.CreateElement(name);
                node.AppendChild(elem);
            }
            elem.InnerText = val;
            return elem;
        }

        public static string GetNodeText(XmlNode node, string xpath, string defaultValue = "")
        {
            string value = defaultValue;
            if (node != null)
            {
                XmlNode temp = node.SelectSingleNode(xpath);
                if (temp != null)
                {
                    value = temp.InnerText;
                }
            }
            return value;
        }

    }
}
