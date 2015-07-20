using MongoDB.Driver;

namespace RedCapped.Core.Extensions
{
    public static class ExtensionMethods
    {
        internal static WriteConcern ToWriteConcern(this QoS qos)
        {
            switch (qos)
            {
                default:
                    return WriteConcern.W1;
                case QoS.Low:
                    return WriteConcern.Acknowledged;
                case QoS.High:
                    return WriteConcern.WMajority;
            }
        }
    }
}
