// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using Cassandra;
    using CassandraQuickstart.CosmosDBUtils;

    internal static class CustomPayloadHelpers
    {
        public static double ExtractRequestChargeFromCustomPayload(RowSet rowSet)
        {
            byte[] valueInBytes = rowSet.Info.IncomingPayload[CosmosDBConstants.CustomPayloadKeys.RequestCharge];
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(valueInBytes);
            }

            return BitConverter.ToDouble(valueInBytes, 0);
        }

        public static double ExtractRequestChargeFromCustomPayload(PreparedStatement preparedStatement)
        {
            byte[] valueInBytes = preparedStatement.IncomingPayload[CosmosDBConstants.CustomPayloadKeys.RequestCharge];
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(valueInBytes);
            }

            return BitConverter.ToDouble(valueInBytes, 0);
        }

        public static Dictionary<string, byte[]> CreateCustomPayload(int? throughputToProvision)
        {
            var outgoingPayload = new Dictionary<string, byte[]>();
            if (throughputToProvision.HasValue)
            {
                outgoingPayload.Add(CosmosDBConstants.CustomPayloadKeys.RequestCharge, Encoding.UTF8.GetBytes(throughputToProvision.ToString()));
            };

            return outgoingPayload;
        }
    }
}
