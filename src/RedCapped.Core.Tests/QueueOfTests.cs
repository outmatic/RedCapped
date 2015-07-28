using System;
using System.Threading;
using NUnit.Framework;

namespace RedCapped.Core.Tests
{
    [TestFixture]
    public class QueueOfTests
    {
        private IQueueOf<string> _sut;
        private QueueFactory _manager;

        public QueueOfTests()
        {
            MongoDbUtils.DropDatabase();
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            MongoDbUtils.DropDatabase();
        }

        [SetUp]
        public void SetUp()
        {
            _manager = new QueueFactory(MongoDbUtils.ConnectionString, MongoDbUtils.DatabaseName);
            _sut = _manager.CreateQueueAsync<string>("testqueue", 4096).Result;
        }

        [Test]
        public async void PublishAsync_can_publish_a_message()
        {
            // WHEN
            var actual = await _sut.PublishAsync("hi!");

            // THEN
            Assert.That(actual, Is.Not.Null);
        }

        [Test]
        public async void PublishAsync_can_publish_a_message_with_different_qos()
        {
            // WHEN
            var actual = await _sut.PublishAsync("hi!", qos: QoS.High);

            // THEN
            Assert.That(actual, Is.Not.Null);
        }

        [Test]
        public void PublishAsync_throws_when_receive_limit_too_low()
        {
            Assert.Throws<ArgumentException>(async () => await _sut.PublishAsync("hi!", 0));
        }

        [Test]
        [Timeout(5000)]
        public async void Subscribe_receives_message()
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
