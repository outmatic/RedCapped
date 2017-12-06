using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace RedCapped.Core
{
    public interface IMongoContext
    {
        Task<bool> CollectionExistsAsync(string collectionName);
        Task CreateCappedCollectionAsync(string collectionName, long maxSize);
        Task<IMongoCollection<BsonDocument>> GetCollectionAsync<T>(string collectionName, bool checkExists);
    }

    internal class MongoContext : IMongoContext
    {
        private const string Prefix = "red";
        private const int CollectionMaxSize = 4096;
        private readonly IMongoDatabase _database;
        private readonly CancellationToken _cancellationToken;

        private static string CollectionFullName(string collectionName)
        {
            return $"{Prefix}.{collectionName}";
        }

        public MongoContext(string connectionString, string dbName, CancellationToken cancellationToken = default(CancellationToken))
        {
            var client = new MongoClient(connectionString);
            _database = client.GetDatabase(dbName);

            _cancellationToken = cancellationToken;
        }

        public async Task<bool> CollectionExistsAsync(string collectionName)
        {
            var options = new ListCollectionsOptions
            {
                Filter = new BsonDocument("name", CollectionFullName(collectionName))
            };

            using (var cursor = await _database.ListCollectionsAsync(options, _cancellationToken))
            {
                if (cursor == null)
                {
                    return false;
                }

                return await cursor.MoveNextAsync(_cancellationToken) && cursor.Current.Any();
            }
        }

        public async Task CreateCappedCollectionAsync(string collectionName, long maxSize)
        {
            var collectionOptions = new CreateCollectionOptions
            {
                Capped = true,
                AutoIndexId = true,
                MaxSize = maxSize > CollectionMaxSize ? maxSize : CollectionMaxSize
            };

            await _database.CreateCollectionAsync(CollectionFullName(collectionName), collectionOptions, _cancellationToken);

            var collection = _database.GetCollection<BsonDocument>(CollectionFullName(collectionName));

            var indexOptions = new CreateIndexOptions
            {
                Background = true
            };

            var builder = Builders<BsonDocument>.IndexKeys;
            var indexKeys = builder.Ascending("h.t")
                .Ascending("h.a");

            await collection.Indexes.CreateOneAsync(indexKeys, indexOptions, _cancellationToken);
        }

        public async Task<IMongoCollection<BsonDocument>> GetCollectionAsync<T>(string collectionName, bool checkExists)
        {
            if (!checkExists || await CollectionExistsAsync(collectionName))
            {
                return _database.GetCollection<BsonDocument>(CollectionFullName(collectionName));
            }

            return null;
        }
    }
}
