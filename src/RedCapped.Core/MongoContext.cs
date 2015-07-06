using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace RedCapped.Core
{
    public class MongoContext : IMongoContext
    {
        private const string Prefix = "red";
        private const int CollectionMaxSize = 4096;
        private readonly Lazy<IMongoClient> _client;
        private readonly Lazy<IMongoDatabase> _database;
        private readonly CancellationToken _cancellationToken;

        private static string CollectionFullName(string collectionName)
        {
            return string.Format("{0}.{1}", Prefix, collectionName);
        }

        public MongoContext(string connectionString, string dbName, CancellationToken cancellationToken = default(CancellationToken))
        {
            _client = new Lazy<IMongoClient>(() => new MongoClient(connectionString));
            _database = new Lazy<IMongoDatabase>(() => _client.Value.GetDatabase(dbName));
            _cancellationToken = cancellationToken;
        }

        public async Task<bool> CollectionExistsAsync(string collectionName)
        {
            var options = new ListCollectionsOptions
            {
                Filter = new BsonDocument("name", CollectionFullName(collectionName))
            };

            using (var cursor = await _database.Value.ListCollectionsAsync(options, _cancellationToken))
            {
                if (cursor == null)
                {
                    return false;
                }

                return await cursor.MoveNextAsync(_cancellationToken) && cursor.Current.Any();
            }
        }

        public async Task CreateCappedCollectionAsync(string collectionName, int maxSize)
        {
            var opt = new CreateCollectionOptions
            {
                Capped = true,
                AutoIndexId = true,
                MaxSize = maxSize > CollectionMaxSize ? maxSize : CollectionMaxSize
            };

            await _database.Value.CreateCollectionAsync(CollectionFullName(collectionName), opt, _cancellationToken);
        }

        public async Task<IMongoCollection<RedCappedMessage<T>>> GetCollectionAsync<T>(string collectionName)
        {
            if (await CollectionExistsAsync(collectionName))
            {
                return _database.Value.GetCollection<RedCappedMessage<T>>(CollectionFullName(collectionName));
            }

            return null;
        }
    }
}
