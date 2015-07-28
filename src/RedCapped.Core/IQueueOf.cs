using System;
using System.Threading.Tasks;

namespace RedCapped.Core
{
    public interface IQueueOf<T>
    {
        bool Subscribed { get; }
        Task<string> PublishAsync(T message, int retryLimit = 3, QoS qos = QoS.Normal);
        void Subscribe(Func<T, bool> handler);
        void Unsubscribe();
    }
}