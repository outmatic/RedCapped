using System.Threading.Tasks;
using MongoDB.Driver;

namespace RedCapped.Core
{
    public interface IMongoContext
    {
        Task<bool> CollectionExistsAsync(string collectionName);
        Task CreateCappedCollectionAsync(string collectionName, int maxSize);
        Task<IMongoCollection<RedCappedMessage<T>>> GetCollectionAsync<T>(string collectionName);
    }
}
