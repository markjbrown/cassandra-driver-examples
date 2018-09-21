// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart.Scenarios
{
    using System;
    using System.Threading.Tasks;
    using Cassandra;

    internal abstract class ScenarioBase
    {
        /// <summary>
        /// Default throughout to specify for database/collection.
        /// </summary>
        internal const int DefaultThroughputForQuickstart = 1000;

        /// <summary>
        /// Gets an instance of the <see cref="ISession"/> object to issue CQL commands.
        /// </summary>
        protected ISession Session { get; private set; }

        /// <summary>
        /// Initializes the default state and connects to the Azure CosmosDB Cassandra API
        /// </summary>
        /// <param name="hostName">Host name of the CosmosDB account, as shown in the "Connection String" section of Azure Portal</param>
        /// <param name="userName">UserName to connect to the CosmosDB account, as shown in the "Connection String" section of Azure Portal</param>
        /// <param name="password">
        /// Password to connect the CosmosDB account, as shown in the "Connection String" section of Azure Portal.
        /// You can use either the "Primary Master ReadWrite" or "Seconday Master ReadWrite" keys here.
        /// </param>
        public async Task InitializeAsync(string hostName, string userName, string password)
        {
            if (this.Session != null)
            {
                throw new InvalidOperationException("Already Initialized..");
            }

            Console.WriteLine($"Connecting to cluster: {hostName}");
            Cluster cluster = ClusterBuilderHelpers.CreateClusterForCosmosDB(hostName, userName, password);
            this.Session = await cluster.ConnectAsync();
            Console.WriteLine("Connecting to cluster: DONE");
        }

        /// <summary>
        /// Executes all the operations defined in the scenario.
        /// </summary>
        public abstract Task RunScenarioAsync();
    }
}
