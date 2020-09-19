namespace HDBCLI
{
    using System.Data;

    interface IOutput
    {
        void Prepare();
        void Write(DataTable result, Session session);
    }
}
