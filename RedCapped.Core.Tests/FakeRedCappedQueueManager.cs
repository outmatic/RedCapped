namespace RedCapped.Core.Tests
{
    public class FakeRedCappedQueueManager : RedCappedQueueManager
    {
        public FakeRedCappedQueueManager(IMongoContext mongoContext)
            : base(mongoContext)
        {

        }
    }
}
