namespace HDBCLI
{
    using System.Data;

    interface IOutput
    {
        void BeforeWrite();
        void AfterWrite();
        void Write(DataTable result, Session session);
    }
}
