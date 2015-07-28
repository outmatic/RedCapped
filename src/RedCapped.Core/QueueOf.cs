using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using RedCapped.Core.Extensions;

namespace RedCapped.Core
{
    public class QueueOf<T> : IQueueOf<T>
    {
        private CancellationTokenSource _cancellationTokenSource;
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly IMongoCollection<BsonDocument> _errorCollection;

        protected internal QueueOf(IMongoCollection<BsonDocument> collection,
            IMongoCollection<BsonDocument> errorCollection)
        {
            _collection = collection;
            _errorCollection = errorCollection;
        }

        public bool Subscribed { get; private set; }

        public void Subscribe(Func<T, bool> handler)
        {
            Task.Factory.StartNew((() => SubscribeInternal(handler)), TaskCreationOptions.LongRunning);
        }

        public void Unsubscribe()
        {
            _cancellationTokenSource?.Cancel();
            Subscribed = false;
        }

        public async Task<string> PublishAsync(T message, int retryLimit = 3, QoS qos = QoS.Normal)
        {
            if (retryLimit < 1)
            {
                throw new ArgumentException($"{nameof(retryLimit)} cannot be less than 1", nameof(retryLimit));
            }

            var msg = new Message<T>(message)
            {
                Header = new Header<T>
                {
                    QoS = qos,
                    SentAt = DateTime.Now,
                    AcknowledgedAt = DateTime.MinValue,
                    RetryLimit = retryLimit
                },
            };

            return await PublishAsyncInternal(msg);
        }

        private async Task<string> PublishAsyncInternal(Message<T> message)
        {
            message.MessageId = ObjectId.GenerateNewId().ToString();

            await
                _collection.WithWriteConcern(message.Header.QoS.ToWriteConcern())
                    .InsertOneAsync(message.ToBsonDocument());

            return message.MessageId;
        }

        private async Task SubscribeInternal(Func<T, bool> handler)
        {
            _cancellationTokenSource = new CancellationTokenSource();

            try
            {
                var findOptions = new FindOptions<BsonDocument>
                {
                    CursorType = CursorType.TailableAwait,
                    NoCursorTimeout = true
                };

                var builder = Builders<BsonDocument>.Filter;
                var filter = builder.Eq("h.t", typeof (T).ToString())
                             & builder.Eq("h.a", DateTime.MinValue);

                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    Subscribed = true;

                    using (var cursor = await
                        _collection.FindAsync(filter, findOptions, _cancellationTokenSource.Token))
                    {
                        await cursor.ForEachAsync(async doc =>
                        {
                            Message<T> item = null;

                            try
                            {
                                item = BsonSerializer.Deserialize<Message<T>>(doc);
                            }
                            catch (Exception)
                            {
                                Debug.WriteLine("Found an offending payload:\n{0}", item.ToJson());

                                return;
                            }

                            if (await AckAsync(item) && !handler(item.Payload))
                            {
                                if (item.Header.RetryCount < item.Header.RetryLimit)
                                {
                                    await PublishAsyncInternal(item);
                                }
                                else
                                {
                                    await
                                        _errorCollection.WithWriteConcern(item.Header.QoS.ToWriteConcern())
                                            .InsertOneAsync(item.ToBsonDocument(), _cancellationTokenSource.Token);
                                }
                            }
                        }, _cancellationTokenSource.Token);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Subscribed = false;

                Debug.WriteLine("Cancellation Requested");
            }
        }

        private async Task<bool> AckAsync(Message<T> message)
        {
            var builder = Builders<BsonDocument>.Filter;
            var filter = builder
                .Eq("_id", ObjectId.Parse(message.MessageId))
                         & builder.Eq("h.a", DateTime.MinValue);
            var update = Builders<BsonDocument>.Update
                .Set("h.a", DateTime.Now)
                .Inc("h.c", 1);

            var result =
                await _collection.WithWriteConcern(message.Header.QoS.ToWriteConcern()).UpdateOneAsync(filter, update);

            return result.MatchedCount == 1 && result.ModifiedCount == 1;
        }
    }
}