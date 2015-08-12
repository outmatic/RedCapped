using MongoDB.Driver;

namespace RedCapped.Core.Tests
{
    public static class MongoDbUtils
    {
        public const string ConnectionString = "mongodb://localhost";
        public const string DatabaseName = "redcappedtest";

        public static void DropDatabase()
        {
            var client = new MongoClient(ConnectionString);
            client.DropDatabaseAsync(DatabaseName).Wait();
        }
    }
}