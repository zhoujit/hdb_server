namespace HDBCLI
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;

    class TextOutput : IOutput
    {
        public TextOutput(string lineSeparator, string colSeparator)
        {
            m_lineSeparator = lineSeparator;
            m_colSeparator = colSeparator;
        }

        public event EventHandler<DataLineReadyEventArgs> DataLineReady;

        public virtual void Write(DataTable result, Session session)
        {
            BeforeWrite();

            List<int> columnWidths = CalcColumnWidth(result, session);
            StringBuilder outputBuffer = new StringBuilder();
            for (int colNo = 0; colNo < result.Columns.Count; colNo++)
            {
                DataColumn col = result.Columns[colNo];
                if (outputBuffer.Length > 0)
                {
                    outputBuffer.Append(m_colSeparator);
                }
                OutputFieldValue(outputBuffer, col.ColumnName, columnWidths[colNo]);
            }
            outputBuffer.Append(m_lineSeparator);
            DataLineReady?.Invoke(null, new DataLineReadyEventArgs(outputBuffer, true, true));

            foreach (DataRow row in result.Rows)
            {
                for (int colNo = 0; colNo < result.Columns.Count; colNo++)
                {
                    if (colNo > 0)
                    {
                        outputBuffer.Append(m_colSeparator);
                    }
                    string value = FormatValue(row, result.Columns[colNo]);
                    OutputFieldValue(outputBuffer, value, columnWidths[colNo]);
                }
                outputBuffer.Append(m_lineSeparator);
                DataLineReady?.Invoke(null, new DataLineReadyEventArgs(outputBuffer, false, true));
            }

            DataLineReady?.Invoke(null, new DataLineReadyEventArgs(outputBuffer, false, false));

            AfterWrite();
        }

        private List<int> CalcColumnWidth(DataTable result, Session session)
        {
            List<int> columnWidths = new List<int>();
            foreach (DataColumn col in result.Columns)
            {
                int width = -1;
                if (!session.OutputCompactMode && session.OutputType != OutputTypeEnum.CSV && session.OutputType != OutputTypeEnum.TabFile)
                {
                    width = col.ColumnName.Length;
                    for (int rowNo = 0; rowNo < 100 && rowNo < result.Rows.Count; rowNo++)
                    {
                        DataRow row = result.Rows[rowNo];
                        string value = FormatValue(row, col);
                        if (value != null)
                        {
                            if (value.Length > width)
                            {
                                width = value.Length;
                            }
                        }
                    }
                }
                columnWidths.Add(width);
            }
            return columnWidths;
        }

        private void OutputFieldValue(StringBuilder outputBuffer, string value, int width)
        {
            outputBuffer.Append(value);
            int realWidth = value.Length;
            if (width > realWidth)
            {
                outputBuffer.Append(' ', width - realWidth);
            }
        }

        private string FormatValue(DataRow row, DataColumn col)
        {
            string value = null;
            if (!row.IsNull(col))
            {
                value = row[col].ToString();
            }
            else
            {
                value = "NULL";
            }
            return value;
        }

        public virtual void BeforeWrite()
        {
        }

        public virtual void AfterWrite()
        {
        }

        private string m_lineSeparator;
        private string m_colSeparator;

    }

    class DataLineReadyEventArgs : EventArgs
    {
        public StringBuilder DataBlock { set; get; }
        public bool IsHeader { set; get; }
        public bool HasNext { set; get; }

        public DataLineReadyEventArgs(StringBuilder dataBlock, bool isHeader, bool hasNext)
        {
            DataBlock = dataBlock;
            IsHeader = isHeader;
            HasNext = hasNext;
        }
    }

}