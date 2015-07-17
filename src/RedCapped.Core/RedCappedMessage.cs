using System;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace RedCapped.Core
{
    public class MessageHeader<T>
    {
        [BsonElement("v")]
        public string Version
        {
            get
            {
                return "1";
            }
            set
            {
                
            }
        }

        [BsonElement("t")]
        public string Type
        {
            get { return typeof(T).FullName; }
        }

        [BsonElement("qos")]
        public QoS QoS { get; set; }

        [BsonElement("sent")]
        public DateTime SentAt { get; set; }

        [BsonElement("ack")]
        public DateTime AcknowledgedAt { get; set; }

        [BsonElement("retry-limit")]
        [BsonIgnoreIfDefault]
        public int RetryLimit { get; set; }

        [BsonElement("retry-count")]
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