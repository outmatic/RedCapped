using System;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;
using NSubstitute;
using NUnit.Framework;
using RedCapped.Core.Tests.Extensions;

namespace RedCapped.Core.Tests
{
    [TestFixture]
    public class QueueOfTests
    {
        private QueueOf<string> _sut;
        private IMongoCollection<RedCappedMessage<string>> _collection;

        [SetUp]
        public void SetUp()
        {
            _collection = Substitute.For<IMongoCollection<RedCappedMessage<string>>>();
            _sut = new FakeQueueOf<string>(_collection);
        }

        [Test]
        public async void QueueOf_Create_index_when_instantiated()
        {
            _collection.Indexes.Received(1).CreateOneAsync(Arg.Any<IndexKeysDefinition<RedCappedMessage<string>>>(),
                Arg.Any<CreateIndexOptions>()).IgnoreAwaitForNSubstituteAssertion();
        }

        [Test]
        public async void QueueOf_Can_publish_messages()
        {
            // WHEN
            var actual = await _sut.PublishAsync("anytopic", "hi!");

            // THEN
            Assert.That(actual, Is.Not.Null);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void QueueOf_Cannot_publish_messages_without_topic()
        {
            var actual = await _sut.PublishAsync("", "hi!");
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void QueueOf_Cannot_subscribe_without_topic()
        {
            _sut.Subscribe("", m => true);
        }

        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public async void QueueOf_Cannot_unsubscribe_without_topic()
        {
            _sut.Unsubscribe("");
        }
    }
}
