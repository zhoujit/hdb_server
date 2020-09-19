namespace HDBCLI
{
    using System;
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
            StringBuilder outputBuffer = new StringBuilder();
            foreach (DataColumn dataColumn in result.Columns)
            {
                if (outputBuffer.Length > 0)
                {
                    outputBuffer.Append(m_lineSeparator);
                }
                outputBuffer.Append(dataColumn.ColumnName);
            }
            DataLineReady?.Invoke(null, new DataLineReadyEventArgs(outputBuffer, true, true));

            foreach (DataRow row in result.Rows)
            {
                bool firstColumn = true;
                foreach (DataColumn dataColumn in result.Columns)
                {
                    if (firstColumn)
                    {
                        firstColumn = false;
                    }
                    else
                    {
                        outputBuffer.Append(m_lineSeparator);
                    }
                    FormatValue(outputBuffer, row, dataColumn);
                }
                outputBuffer.Append(m_lineSeparator);
                DataLineReady?.Invoke(null, new DataLineReadyEventArgs(outputBuffer, false, true));
            }

            DataLineReady?.Invoke(null, new DataLineReadyEventArgs(outputBuffer, false, false));
        }

        private void FormatValue(StringBuilder outputBuffer, DataRow row, DataColumn dataColumn)
        {
            if (!row.IsNull(dataColumn))
            {
                outputBuffer.Append(row[dataColumn]);
            }
        }

        public virtual void Prepare()
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