using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace RedCapped.Core
{
    public class Message<T>
    {
        public Message(T payload)
        {
            Header = new Header<T>();
            Payload = payload;
        }

        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string MessageId { get; set; }

        [BsonElement("h")]
        public Header<T> Header { get; set; }

        [BsonElement("p")]
        public T Payload { get; set; }
    }
}