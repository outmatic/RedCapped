using MongoDB.Bson;
using MongoDB.Driver;

namespace RedCapped.Core.Tests
{
    public class FakeQueueFactory : QueueFactory
    {
        public FakeQueueFactory(IMongoContext mongoContext)
            : base(mongoContext)
        {
        }
    }

    public class FakeQueueOf<T> : QueueOf<T>
    {
        public FakeQueueOf(IMongoCollection<BsonDocument> collection, IMongoCollection<BsonDocument> errorCollection)
            : base(collection, errorCollection)
        {
        }
    }
}