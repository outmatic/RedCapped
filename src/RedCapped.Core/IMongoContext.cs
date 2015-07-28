using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace RedCapped.Core
{
    public interface IMongoContext
    {
        Task<bool> CollectionExistsAsync(string collectionName);
        Task CreateCappedCollectionAsync(string collectionName, long maxSize);
        Task<IMongoCollection<BsonDocument>> GetCollectionAsync<T>(string collectionName, bool checkExists);
    }
}
