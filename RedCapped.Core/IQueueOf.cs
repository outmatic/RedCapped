using System;
using System.Threading.Tasks;

namespace RedCapped.Core
{
    public interface IQueueOf<T>
    {
        Task<string> PublishAsync(string topic, T message);
        void SubscribeAsync(string topic, Func<T, bool> handler);
        void Unsubscribe(string topic);
    }
}
