using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;

namespace RedCapped.Core.Tests
{
    [TestFixture(Category = "Integration")]
    public class QueueOfTests
    {
        [SetUp]
        public void SetUp()
        {
            MongoDbUtils.DropDatabase();
            _manager = new QueueFactory(MongoDbUtils.ConnectionString, MongoDbUtils.DatabaseName);
            _sut = _manager.CreateQueueAsync<string>("testqueue", 4096).Result;
        }

        private IQueueOf<string> _sut;
        private QueueFactory _manager;

        [OneTimeTearDown]
        public void FixtureTearDown()
        {
            MongoDbUtils.DropDatabase();
        }

        [Test]
        public async Task PublishAsync_can_publish_a_message()
        {
            // WHEN
            var actual = await _sut.PublishAsync("hi!");

            // THEN
            Assert.That(actual, Is.Not.Null);
        }

        [Test]
        public async Task PublishAsync_can_publish_a_message_with_different_qos()
        {
            // WHEN
            var actual = await _sut.PublishAsync("hi!", qos: QoS.High);

            // THEN
            Assert.That(actual, Is.Not.Null);
        }

        private async Task Publish(string msg, int retryLimit)
        {
            await _sut.PublishAsync(msg, retryLimit);
        }

        [Test]
        public void PublishAsync_throws_when_receive_limit_too_low()
        {
            Assert.Throws<AggregateException>(() => { _sut.PublishAsync("Hi!", 0).Wait(); });
        }

        [Test]
        [Timeout(5000)]
        public async Task Subscribe_receives_message()
        {
            // GIVEN
            const string expected = "hi I'm a message!";
            var id = await _sut.PublishAsync(expected);

            // WHEN
            string actual = null;
            _sut.Subscribe(m =>
            {
                actual = m;
                return true;
            });

            // THEN
            Assert.That(id, Is.Not.Null);

            while (actual == null)
            {
                Thread.Sleep(100);
            }
            Assert.That(actual, Is.EqualTo(expected));
        }

        [Test]
        public void Unsubscribe_can_be_safely_called_multiple_times()
        {
            _sut.Unsubscribe();
            _sut.Unsubscribe();
            _sut.Unsubscribe();
        }
    }
}