using System.Threading.Tasks;

namespace RedCapped.Core.Tests.Extensions
{
    public static class NSubstituteHelpers
    {
        public static void IgnoreAwaitForNSubstituteAssertion(this Task task)
        {
        }
    }
}