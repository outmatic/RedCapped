using MongoDB.Driver;

namespace RedCapped.Core.Tests
{
    public class FakeRedCappedQueueManager : RedCappedQueueManager
    {
        public FakeRedCappedQueueManager(IMongoContext mongoContext)
            : base(mongoContext)
        {

        }
    }

    public class FakeQueueOf<T> : QueueOf<T>
    {
        public FakeQueueOf(IMongoCollection<RedCappedMessage<T>> collection)
            : base(collection)
        {

        }
    }
}
