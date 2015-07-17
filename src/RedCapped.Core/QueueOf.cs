using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace RedCapped.Core
{
    public class QueueOf<T> : IQueueOf<T>
    {
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _cancellationTokenList;
        private readonly IMongoCollection<RedCappedMessage<T>> _collection;
        private readonly IMongoCollection<BsonDocument> _safeCollection;
        private readonly IMongoCollection<BsonDocument> _errorCollection;

        protected internal QueueOf(IMongoCollection<RedCappedMessage<T>> collection,
            IMongoCollection<BsonDocument> safeCollection,
            IMongoCollection<BsonDocument> errorCollection)
        {
            _collection = collection;
            _safeCollection = safeCollection;
            _errorCollection = errorCollection;
            _cancellationTokenList = new ConcurrentDictionary<string, CancellationTokenSource>();
            CreateIndex();
        }

        public bool Subscribed { get; private set; }

        public void Subscribe(string topic, Func<T, bool> handler)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentNullException("topic");
            }

            Subscribed = true;

            Task.Factory.StartNew((() => SubscribeInternal(topic, handler)), TaskCreationOptions.LongRunning);
        }

        public void Unsubscribe(string topic)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentNullException("topic");
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
                throw new ArgumentNullException("topic");
            }

            if (retryLimit < 1)
            {
                throw new ArgumentException("retryLimit cannot be less than 1", "retryLimit");
            }

            var msg = new RedCappedMessage<T>(message)
            {
                Header = new MessageHeader<T>
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

        private async Task<string> PublishAsyncInternal(RedCappedMessage<T> message)
        {
            message.MessageId = ObjectId.GenerateNewId().ToString();

            await
                _collection.WithWriteConcern(QosToWriteConcern(message.Header.QoS)).InsertOneAsync(message);

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
                var filter = builder.Eq("header.t", typeof(T).ToString())
                    & builder.Eq("header.ack", DateTime.MinValue)
                    & builder.Eq("topic", topic);

                while (!cancellationToken.IsCancellationRequested)
                {
                    using (var cursor = await
                        _safeCollection.FindAsync(filter, findOptions, cancellationToken.Token))
                    {
                        await cursor.ForEachAsync(async doc =>
                        {
                            var error = false;

                            RedCappedMessage<T> item = null;

                            try
                            {
                                item = BsonSerializer.Deserialize<RedCappedMessage<T>>(doc);
                            }
                            catch (Exception)
                            {
                                Debug.WriteLine("Found an unexpected payload:\n{0}", item.ToJson());
                            }

                            if (item != null && await AckAsync(item.MessageId))
                            {
                                if (!handler(item.Message))
                                {
                                    if (item.Header.RetryCount < item.Header.RetryLimit)
                                    {
                                        await PublishAsyncInternal(item);
                                    }
                                    else
                                    {
                                        error = true;
                                    }
                                }
                            }
                            else
                            {
                                error = true;
                            }

                            if (error)
                            {
                                await
                                    _errorCollection.WithWriteConcern(QosToWriteConcern(item.Header.QoS))
                                        .InsertOneAsync(item.ToBsonDocument(), cancellationToken.Token);
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

            await _collection.Indexes.CreateOneAsync(Builders<RedCappedMessage<T>>.IndexKeys
                .Ascending(x => x.Header.Type)
                .Ascending(x => x.Header.AcknowledgedAt)
                .Ascending(x => x.Topic), options);
        }

        private async Task<bool> AckAsync(string messageId)
        {
            var result = await _collection.UpdateOneAsync(
                x => x.MessageId == messageId
                     & x.Header.AcknowledgedAt == DateTime.MinValue,
                Builders<RedCappedMessage<T>>.Update
                    .Set(x => x.Header.AcknowledgedAt, DateTime.Now)
                    .Inc(x => x.Header.RetryCount, 1)
                );

            return result.MatchedCount == 1 && result.ModifiedCount == 1;
        }

        private static WriteConcern QosToWriteConcern(QoS qos)
        {
            WriteConcern w;

            switch (qos)
            {
                default:
                    w = WriteConcern.Acknowledged;
                    break;
                case QoS.Low:
                    w = WriteConcern.Unacknowledged;
                    break;
                case QoS.High:
                    w = WriteConcern.WMajority;
                    break;
            }

            return w;
        }
    }
}