using System;
using System.Threading.Tasks;

namespace RedCapped.Core
{
    public interface IQueueOf<T>
    {
        bool Subscribed { get; }
        Task<string> PublishAsync(string topic, T message, int receiveLimit = 3);
        void Subscribe(string topic, Func<T, bool> handler);
        void Unsubscribe(string topic);
    }
}