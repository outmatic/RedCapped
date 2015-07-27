using System;
using MongoDB.Bson.Serialization.Attributes;

namespace RedCapped.Core
{
    public class Header<T>
    {
        [BsonElement("v")]
        public string Version => "1";

        [BsonElement("t")]
        public string Type => typeof(T).FullName;

        [BsonElement("qos")]
        public QoS QoS { get; set; }

        [BsonElement("sent")]
        public DateTime SentAt { get; set; }

        [BsonElement("ack")]
        public DateTime AcknowledgedAt { get; set; }

        [BsonElement("rl")]
        [BsonIgnoreIfDefault]
        public int RetryLimit { get; set; }

        [BsonElement("rc")]
        public int RetryCount { get; set; }
    }
}
