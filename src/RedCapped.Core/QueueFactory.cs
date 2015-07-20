using System;
using System.Threading.Tasks;

namespace RedCapped.Core
{
    public class QueueFactory
    {
        private readonly Lazy<IMongoContext> _mongoContext;

        protected QueueFactory(IMongoContext mongoContext)
        {
            _mongoContext = new Lazy<IMongoContext>(() => mongoContext);
        }

        public QueueFactory(string connectionString, string dbName)
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
            var collection = await _mongoContext.Value.GetCappedCollectionAsync<T>(queueName);
            if (collection == null)
            {
                return null;
            }

            var safeCollection = _mongoContext.Value.GetCollection(queueName);
            var errorCollection = _mongoContext.Value.GetCollection(string.Format("{0}_err", queueName));
            
            return new QueueOf<T>(safeCollection, errorCollection);
        }
    }
}
