using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra_Quickstart_101.CosmosDBUtils
{
    internal static class CosmosDBConstants
    {
        public static class CustomPayloadKeys
        {
            public const string RequestCharge = "RequestCharge";
        }

        public static class CustomOptions
        {
            public const string CosmosDBProvisionedThroughput = "cosmosdb_provisioned_throughput";
        }
    }
}
