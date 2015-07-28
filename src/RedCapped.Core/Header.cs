using System;
using MongoDB.Bson.Serialization.Attributes;

namespace RedCapped.Core
{
    public class Header<T>
    {
        [BsonElement("t")]
        public string Type => typeof(T).FullName;

        [BsonElement("q")]
        public QoS QoS { get; set; }

        [BsonElement("s")]
        public DateTime SentAt { get; set; }

        [BsonElement("a")]
        public DateTime AcknowledgedAt { get; set; }

        [BsonElement("l")]
        public int RetryLimit { get; set; }

        [BsonElement("c")]
        public int RetryCount { get; set; }
    }
}
