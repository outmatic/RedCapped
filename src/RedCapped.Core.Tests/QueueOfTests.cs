using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using NSubstitute;
using NUnit.Framework;

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
        }

        [Test]
        public void QueueOf_Create_index_when_instantiated()
        {
            _sut = new FakeQueueOf<string>(_collection);

            _collection.Indexes.Received(1).CreateOneAsync(Arg.Any<IndexKeysDefinition<RedCappedMessage<string>>>(), Arg.Any<CreateIndexOptions>());
        }
    }
}
