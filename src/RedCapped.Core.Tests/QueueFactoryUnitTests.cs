using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using NSubstitute;
using NUnit.Framework;
using RedCapped.Core.Tests.Extensions;

namespace RedCapped.Core.Tests
{
    [TestFixture]
    public class QueueFactoryUnitTests
    {
        [SetUp]
        public void SetUp()
        {
            _collection = Substitute.For<IMongoCollection<BsonDocument>>();
            _mongoContext = Substitute.For<IMongoContext>();
            _mongoContext.GetCollectionAsync<BsonDocument>("anyqueue", true)
                .Returns(Task.FromResult(_collection));
            _mongoContext.CreateCappedCollectionAsync("anyqueue", 1000)
                .Returns(Task.FromResult(_collection));
        }

        private FakeQueueFactory _sut;
        private IMongoContext _mongoContext;
        private IMongoCollection<BsonDocument> _collection;

        [Test]
        public async void CreateQueueAsync_creates_a_new_queue_by_checking_if_it_exists()
        {
            // GIVEN
            var expected = typeof (IQueueOf<string>);

            _mongoContext.CollectionExistsAsync("anyqueue")
                .Returns(Task.FromResult(false));

            _sut = new FakeQueueFactory(_mongoContext);

            // WHEN
            var actual = await _sut.CreateQueueAsync<string>("anyqueue", 1000);

            // THEN
            _mongoContext.Received(1).CollectionExistsAsync("anyqueue").IgnoreAwaitForNSubstituteAssertion();
            Assert.That(actual, Is.InstanceOf(expected));
        }

        [Test]
        public async void CreateQueueAsync_returns_existing_queue_if_it_exists()
        {
            // GIVEN
            var expected = typeof (IQueueOf<string>);

            _mongoContext.CollectionExistsAsync("anyqueue")
                .Returns(Task.FromResult(true));

            _sut = new FakeQueueFactory(_mongoContext);

            // WHEN
            var actual = await _sut.CreateQueueAsync<string>("anyqueue", 1000);

            // THEN
            _mongoContext.Received(1).CollectionExistsAsync("anyqueue").IgnoreAwaitForNSubstituteAssertion();
            Assert.That(actual, Is.InstanceOf(expected));
        }

        [Test]
        public async void GetQueueAsync_returns_existent_queue()
        {
            // GIVEN
            var expected = typeof (IQueueOf<string>);

            _sut = new FakeQueueFactory(_mongoContext);

            // WHEN
            var actual = await _sut.GetQueueAsync<string>("anyqueue");

            // THEN
            Assert.That(actual, Is.InstanceOf(expected));
        }

        [Test]
        public async void GetQueueAsync_returns_null_for_unexistent_queue()
        {
            // GIVEN
            _mongoContext.GetCollectionAsync<BsonDocument>("anyqueue", true)
                .Returns(Task.FromResult((IMongoCollection<BsonDocument>) null));

            _sut = new FakeQueueFactory(_mongoContext);

            // WHEN
            var actual = await _sut.GetQueueAsync<string>("anyqueue");

            // THEN
            Assert.That(actual, Is.Null);
        }
    }
}