using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace RedCapped.Core
{
    public class MessageHeader<T>
    {
        [BsonElement("_v")]
        private const string Version = "1";

        [BsonElement("_type")]
        public string Type
        {
            get { return typeof(T).FullName; }
        }

        [BsonElement("sent")]
        public DateTime SentAt { get; set; }

        [BsonElement("ack")]
        public DateTime AcknowledgedAt { get; set; }

        [BsonElement("retry")]
        public int RetryCount { get; set; }
    }

    public class RedCappedMessage<T>
    {
        public RedCappedMessage(T message)
        {
            Header = new MessageHeader<T>();
            Message = message;
        }

        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string MessageId { get; set; }

        [BsonElement("header")]
        public MessageHeader<T> Header { get; set; }

        [BsonElement("topic")]
        public string Topic { get; set; }

        [BsonElement("payload")]
        public T Message { get; set; }
    }
}