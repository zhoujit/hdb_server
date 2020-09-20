namespace HDBPublic
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;

    public partial class SQLStatement
    {
        public event EventHandler<ImpProgressEventArgs> ImpProgress;

        void ImportTable(string tableName, string fileName, string lineSeparator = "\n", string colSeparator = ",")
        {
            const int MAXBUFFERSIZE = 20;//;1024 * 1024 * 4;
            char[] buffer = new char[MAXBUFFERSIZE];
            long grandTotalCount = 0;
            long currentTotalCount = 0;
            List<string> columns = new List<string>();
            List<Dictionary<string, object>> fieldConditions = new List<Dictionary<string, object>>();
            using (StreamReader reader = new StreamReader(fileName))
            {
                int offset = 0;
                int readedCount = 1;
                while (readedCount > 0)
                {
                    readedCount = reader.Read(buffer, offset, MAXBUFFERSIZE - offset);
                    int totalAvailableSize = readedCount + offset;
                    string text = new string(buffer, 0, totalAvailableSize);

                    int lineEndIndex = text.IndexOf(lineSeparator);
                    if (lineEndIndex < 0)
                    {
                        if (reader.Peek() == -1)
                        {
                            // File end.
                            lineEndIndex = totalAvailableSize;
                        }
                        else
                        {
                            throw new Exception("Line is too long or line separator is mismatch.");
                        }
                    }

                    int startIndex = 0;
                    while (lineEndIndex >= 0)
                    {
                        string line = new string(buffer, startIndex, lineEndIndex - startIndex).TrimEnd(new char[] { '\r', '\n' });
                        startIndex = lineEndIndex + lineSeparator.Length;
                        if (columns.Count == 0)
                        {
                            // First line should be header.
                            string[] columnNames = line.Split(new string[] { colSeparator }, StringSplitOptions.None);
                            foreach (string columnName in columnNames)
                            {
                                if (!FieldNameRegex.IsMatch(columnName))
                                {
                                    throw new Exception($"Invalid field name: {columnName}");
                                }
                                columns.Add(columnName);
                            }
                            if (columns.Count == 0)
                            {
                                throw new Exception("Must provide the field name list in the first line.");
                            }
                        }
                        else
                        {
                            if (line.Length > 0)
                            {
                                grandTotalCount++;
                                currentTotalCount++;
                                string[] items = line.Split(new string[] { colSeparator }, StringSplitOptions.None);
                                Dictionary<string, object> valueMap = new Dictionary<string, object>();
                                for (int i = 0; i < items.Length && i < columns.Count; i++)
                                {
                                    valueMap.Add(columns[i], items[i]);
                                }
                                fieldConditions.Add(valueMap);

                                if (fieldConditions.Count % 10000 == 0)
                                {
                                    m_dbClient.Add(tableName, fieldConditions);
                                    fieldConditions.Clear();
                                }
                                if (currentTotalCount % 100000 == 0)
                                {
                                    ImpProgress?.Invoke(null, new ImpProgressEventArgs(grandTotalCount, currentTotalCount, true));
                                    currentTotalCount = 0;
                                }
                            }
                        }
                        if (lineEndIndex + 1 >= text.Length)
                        {
                            lineEndIndex = -1;
                        }
                        else
                        {
                            lineEndIndex = text.IndexOf(lineSeparator, lineEndIndex + 1);
                        }
                    }

                    int availableCharacterCount = totalAvailableSize - startIndex;
                    for (int charIndex = 0; charIndex < availableCharacterCount; charIndex++)
                    {
                        buffer[charIndex] = buffer[charIndex + startIndex];
                    }
                    offset = availableCharacterCount < 0 ? 0 : availableCharacterCount;
                }
            }

            if (fieldConditions.Count > 0)
            {
                m_dbClient.Add(tableName, fieldConditions);
                fieldConditions.Clear();
            }
            ImpProgress?.Invoke(null, new ImpProgressEventArgs(grandTotalCount, currentTotalCount, false));
        }
    }

    public class ImpProgressEventArgs : EventArgs
    {
        public long GrandTotalCount { get; }
        public long CurrentTotalCount { get; }
        public bool HasNext { get; }

        public ImpProgressEventArgs(long grandTotalCount, long currentTotalCount, bool hasNext)
        {
            GrandTotalCount = grandTotalCount;
            CurrentTotalCount = currentTotalCount;
            HasNext = hasNext;
        }
    }
}