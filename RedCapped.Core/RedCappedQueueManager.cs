﻿using System;
using System.Threading.Tasks;

namespace RedCapped.Core
{
    public class RedCappedQueueManager
    {
        private readonly Lazy<IMongoContext> _mongoContext;

        public RedCappedQueueManager(string connectionString, string dbName)
        {
            _mongoContext = new Lazy<IMongoContext>(() => new MongoContext(connectionString, dbName));
        }

        public async Task<QueueOf<T>> CreateQueue<T>(string queueName, int sizeInBytes) where T : class
        {
            if (await _mongoContext.Value.CollectionExistsAsync(queueName))
            {
                return await GetQueue<T>(queueName);
            }

            await _mongoContext.Value.CreateCappedCollectionAsync(queueName, sizeInBytes);

            var queue = await GetQueue<T>(queueName);
            queue.CreateIndex();

            return queue;
        }

        public async Task<QueueOf<T>> GetQueue<T>(string queueName) where T : class
        {
            var queue = await _mongoContext.Value.GetCollectionAsync<T>(queueName);
            return queue != null ? new QueueOf<T>(queue) : null;
        }
    }
}
