using System;
using System.Threading;
using NUnit.Framework;

namespace RedCapped.Core.Tests
{
    [TestFixture]
    public class QueueOfTests
    {
        private IQueueOf<string> _sut;
        private RedCappedQueueManager _manager;

        [SetUp]
        public void SetUp()
        {
            _manager = new RedCappedQueueManager("mongodb://localhost", "redcappedtest");
            _sut = _manager.CreateQueueAsync<string>("testqueue", 4096).Result;
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
        public void QueueOf_Cannot_subscribe_without_topic()
        {
            _sut.Subscribe("", m => true);
        }

        [Test]
        public void QueueOf_Subscribe_and_handle_message()
        {
            // GIVEN
            const string expected = "hi I'm a message!";

            var id = _sut.PublishAsync("anytopic", expected).Result;

            string actual = null;

            // WHEN
            _sut.Subscribe("anytopic", m =>
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
        [ExpectedException(typeof(ArgumentNullException))]
        public void QueueOf_Cannot_unsubscribe_without_topic()
        {
            _sut.Unsubscribe("");
        }
    }
}