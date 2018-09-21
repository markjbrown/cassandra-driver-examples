// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Cassandra;
    using CassandraQuickstart.CosmosDBUtils;

    internal static class CreateTableHelpers
    {
        public static Task<RowSet> CreateTableAsync(ISession session, string cqlString)
        {
            SimpleStatement createTableStatement =new SimpleStatement(cqlString);
            return session.ExecuteAsync(createTableStatement);
        }

        public static Task<RowSet> CreateTableWithThroughputInCQLAsync(
            ISession session, 
            string cqlString, 
            int throughputToProvision)
        {
            SimpleStatement createTableStatement = new SimpleStatement(
                cqlString + $" WITH {CosmosDBConstants.CustomOptions.CosmosDBProvisionedThroughput}={throughputToProvision}");
            return session.ExecuteAsync(createTableStatement);
        }

        public static Task<RowSet> CreateKeyspaceWithThroughputInCustomPayloadAsync(ISession session, string cqlString, int throughputToProvision)
        {
            SimpleStatement createTableStatement = new SimpleStatement(cqlString);
            Dictionary<string, byte[]> customPayload = CustomPayloadHelpers.CreateCustomPayload(throughputToProvision);
            createTableStatement.SetOutgoingPayload(customPayload);

            return session.ExecuteAsync(createTableStatement);
        }
    }
}
