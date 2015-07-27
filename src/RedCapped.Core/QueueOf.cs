using System;
using System.Collections.Concurrent;
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
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _cancellationTokenList;
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly IMongoCollection<BsonDocument> _errorCollection;

        protected internal QueueOf(IMongoCollection<BsonDocument> collection,
            IMongoCollection<BsonDocument> errorCollection)
        {
            _collection = collection;
            _errorCollection = errorCollection;
            _cancellationTokenList = new ConcurrentDictionary<string, CancellationTokenSource>();
            CreateIndex();
        }

        public bool Subscribed { get; private set; }

        public void Subscribe(string topic, Func<T, bool> handler)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentNullException(nameof(topic));
            }

            Subscribed = true;

            Task.Factory.StartNew((() => SubscribeInternal(topic, handler)), TaskCreationOptions.LongRunning);
        }

        public void Unsubscribe(string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentNullException(nameof(topic));
            }

            CancellationTokenSource t;
            if (!_cancellationTokenList.TryRemove(topic, out t))
            {
                return;
            }

            Subscribed = false;
            t.Cancel();
        }

        public async Task<string> PublishAsync(string topic, T message, int retryLimit = 3, QoS qos = QoS.Normal)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentNullException(nameof(topic));
            }

            if (retryLimit < 1)
            {
                throw new ArgumentException("retryLimit cannot be less than 1", nameof(retryLimit));
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
                Topic = topic
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

        private async Task SubscribeInternal(string topic, Func<T, bool> handler)
        {
            var cancellationToken = new CancellationTokenSource();
            _cancellationTokenList[topic] = cancellationToken;

            try
            {
                var findOptions = new FindOptions<BsonDocument>
                {
                    CursorType = CursorType.TailableAwait,
                    NoCursorTimeout = true
                };

                var builder = Builders<BsonDocument>.Filter;
                var filter = builder.Eq("h.t", typeof (T).ToString())
                             & builder.Eq("h.ack", DateTime.MinValue)
                             & builder.Eq("t", topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    using (var cursor = await
                        _collection.FindAsync(filter, findOptions, cancellationToken.Token))
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
                                            .InsertOneAsync(item.ToBsonDocument(), cancellationToken.Token);
                                }
                            }
                        }, cancellationToken.Token);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Debug.WriteLine("Cancellation Requested");
            }
        }

        private async void CreateIndex()
        {
            var options = new CreateIndexOptions
            {
                Background = true
            };

            var builder = Builders<BsonDocument>.IndexKeys;
            var indexKeys = builder.Ascending("h.t")
                .Ascending("h.ack")
                .Ascending("t");

            await _collection.Indexes.CreateOneAsync(indexKeys, options);
        }

        private async Task<bool> AckAsync(Message<T> message)
        {
            var builder = Builders<BsonDocument>.Filter;
            var filter = builder
                .Eq("_id", ObjectId.Parse(message.MessageId))
                         & builder.Eq("h.ack", DateTime.MinValue);
            var update = Builders<BsonDocument>.Update
                .Set("h.ack", DateTime.Now)
                .Inc("h.rc", 1);

            var result =
                await _collection.WithWriteConcern(message.Header.QoS.ToWriteConcern()).UpdateOneAsync(filter, update);

            return result.MatchedCount == 1 && result.ModifiedCount == 1;
        }
    }
}