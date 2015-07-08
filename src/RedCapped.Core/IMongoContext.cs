using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace RedCapped.Core
{
    public interface IMongoContext
    {
        Task<bool> CollectionExistsAsync(string collectionName);
        Task CreateCappedCollectionAsync(string collectionName, int maxSize);
        Task<IMongoCollection<RedCappedMessage<T>>> GetCollectionAsync<T>(string collectionName);
        IMongoCollection<BsonDocument> GetCollection(string collectionName);
    }
}
