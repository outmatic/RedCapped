using System.Threading.Tasks;
using MongoDB.Bson;

namespace RedCapped.Core
{
    public class QueueFactory
    {
        private readonly IMongoContext _mongoContext;

        protected QueueFactory(IMongoContext mongoContext)
        {
            _mongoContext = mongoContext;
        }

        public QueueFactory(string connectionString, string dbName)
        {
            _mongoContext = new MongoContext(connectionString, dbName);
        }

        public async Task<IQueueOf<T>> CreateQueueAsync<T>(string queueName, int sizeInBytes) where T : class
        {
            if (await _mongoContext.CollectionExistsAsync(queueName))
            {
                return await GetQueueAsync<T>(queueName);
            }

            await _mongoContext.CreateCappedCollectionAsync(queueName, sizeInBytes);

            return await GetQueueAsync<T>(queueName);
        }

        public async Task<IQueueOf<T>> GetQueueAsync<T>(string queueName) where T : class
        {
            var collection = await _mongoContext.GetCollectionAsync<BsonDocument>(queueName, true);

            if (collection == null)
            {
                return null;
            }

            var errorCollection = await _mongoContext.GetCollectionAsync<BsonDocument>($"{queueName}_err", false);
            
            return new QueueOf<T>(collection, errorCollection);
        }
    }
}
