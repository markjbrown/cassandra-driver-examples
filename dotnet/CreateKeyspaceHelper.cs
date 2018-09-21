// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Cassandra;
    using CassandraQuickstart.CosmosDBUtils;

    /// <summary>
    /// Helper class for all functions/variations to CreateKeyspace command
    /// </summary>
    internal static class CreateKeyspaceHelper
    {
        public static Task<RowSet> CreateKeyspaceAsync(ISession session, string keyspaceName)
        {
            SimpleStatement createKeyspaceStatement = 
                new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {keyspaceName} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'datacenter1' : 1 }}");
            return session.ExecuteAsync(createKeyspaceStatement);
        }

        public static Task<RowSet> CreateKeyspaceWithThroughputInCQLAsync(
            ISession session, 
            string keyspaceName, 
            int throughputToProvision)
        {
            SimpleStatement createKeyspaceStatement = new SimpleStatement(
                $"CREATE KEYSPACE IF NOT EXISTS {keyspaceName} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'datacenter1' : 1 }} WITH" +
                $" {CosmosDBConstants.CustomOptions.CosmosDBProvisionedThroughput}={throughputToProvision}");
            return session.ExecuteAsync(createKeyspaceStatement);
        }

        public static Task<RowSet> CreateKeyspaceWithThroughputInCustomPayloadAsync(
            ISession session, 
            string keyspaceName, 
            int throughputToProvision)
        {
            SimpleStatement createKeyspaceStatement 
                = new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {keyspaceName} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', 'datacenter1' : 1 }}");
            Dictionary<string, byte[]> customPayload = CustomPayloadHelpers.CreateCustomPayload(throughputToProvision);
            createKeyspaceStatement.SetOutgoingPayload(customPayload);

            return session.ExecuteAsync(createKeyspaceStatement);
        }
    }
}
