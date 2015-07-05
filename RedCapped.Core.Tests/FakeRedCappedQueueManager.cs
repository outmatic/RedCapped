using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
