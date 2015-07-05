using System;
using System.Threading.Tasks;

namespace RedCapped.Core
{
    public class RedCappedQueueManager
    {
        private readonly Lazy<IMongoContext> _mongoContext;

        protected RedCappedQueueManager(IMongoContext mongoContext)
        {
            _mongoContext = new Lazy<IMongoContext>(() => mongoContext);
        }

        public RedCappedQueueManager(string connectionString, string dbName)
        {
            _mongoContext = new Lazy<IMongoContext>(() => new MongoContext(connectionString, dbName));
        }

        public async Task<IQueueOf<T>> CreateQueueAsync<T>(string queueName, int sizeInBytes) where T : class
        {
            if (await _mongoContext.Value.CollectionExistsAsync(queueName))
            {
                return await GetQueueAsync<T>(queueName);
            }

            await _mongoContext.Value.CreateCappedCollectionAsync(queueName, sizeInBytes);

            return await GetQueueAsync<T>(queueName);
        }

        public async Task<IQueueOf<T>> GetQueueAsync<T>(string queueName) where T : class
        {
            var queue = await _mongoContext.Value.GetCollectionAsync<T>(queueName);
            return queue != null ? new QueueOf<T>(queue) : null;
        }
    }
}
