using System.Threading.Tasks;
using MongoDB.Driver;
using NSubstitute;
using NUnit.Framework;
using RedCapped.Core.Tests.Extensions;

namespace RedCapped.Core.Tests
{
    [TestFixture]
    public class RedCappedQueueManagerTests
    {
        private FakeRedCappedQueueManager _sut;
        private IMongoContext _mongoContext;
        private IMongoCollection<RedCappedMessage<string>> _collection;

        [SetUp]
        public void SetUp()
        {
            _collection = Substitute.For<IMongoCollection<RedCappedMessage<string>>>();
            _mongoContext = Substitute.For<IMongoContext>();
            _mongoContext.GetCollectionAsync<string>("anyqueue")
                .Returns(Task.FromResult(_collection));
            _mongoContext.CreateCappedCollectionAsync("anyqueue", 1000)
                .Returns(Task.FromResult(_collection));
        }

        [Test]
        public async void RedCappedQueueManager_Can_get_existent_queue()
        {
            // GIVEN
            var expected = typeof(IQueueOf<string>);

            _sut = new FakeRedCappedQueueManager(_mongoContext);

            // WHEN
            var actual = await _sut.GetQueueAsync<string>("anyqueue");

            // THEN
            Assert.That(actual, Is.InstanceOf(expected));
        }

        [Test]
        public async void RedCappedQueueManager_Returns_null_for_non_existent_queues()
        {
            // GIVEN
            _mongoContext.GetCollectionAsync<string>("anyqueue")
                .Returns(Task.FromResult((IMongoCollection<RedCappedMessage<string>>)null));

            _sut = new FakeRedCappedQueueManager(_mongoContext);

            // WHEN
            var actual = await _sut.GetQueueAsync<string>("anyqueue");

            // THEN
            Assert.That(actual, Is.Null);
        }

        [Test]
        public async void RedCappedQueueManager_Can_create_queue()
        {
            // GIVEN
            var expected = typeof(IQueueOf<string>);

            _mongoContext.CollectionExistsAsync("anyqueue")
                .Returns(Task.FromResult(false));

            _sut = new FakeRedCappedQueueManager(_mongoContext);

            // WHEN
            var actual = await _sut.CreateQueueAsync<string>("anyqueue", 1000);

            // THEN
            _mongoContext.Received(1).CollectionExistsAsync("anyqueue").IgnoreAwaitForNSubstituteAssertion();
            Assert.That(actual, Is.InstanceOf(expected));
        }

        [Test]
        public async void RedCappedQueueManager_Create_an_existing_queue_returns_existing_queue()
        {
            // GIVEN
            var expected = typeof(IQueueOf<string>);

            _mongoContext.CollectionExistsAsync("anyqueue")
                .Returns(Task.FromResult(true));

            _sut = new FakeRedCappedQueueManager(_mongoContext);

            // WHEN
            var actual = await _sut.CreateQueueAsync<string>("anyqueue", 1000);

            // THEN
            _mongoContext.Received(1).CollectionExistsAsync("anyqueue").IgnoreAwaitForNSubstituteAssertion();
            Assert.That(actual, Is.InstanceOf(expected));
        }
    }
}
