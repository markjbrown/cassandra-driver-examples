﻿// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart
{
    using System;
    using System.Net.Security;
    using System.Security.Authentication;
    using System.Security.Cryptography.X509Certificates;
    using Cassandra;
    using CassandraQuickstart.CosmosDBUtils;

    internal static class ClusterBuilderHelpers
    {
        /// <summary>
        /// The default port for CosmosDB Cassandra API.
        /// </summary>
        private static int CosmosDBCassandraPort = 10350;

        /// <summary>
        /// Default page size in Cassandra in set to 5000, which means the driver will only request 5000 rows in any page.
        /// CosmosDB supports dynamic page size, which allows the server to choose the number of rows to be returned.
        /// Setting to Int.MaxValue to override this setting
        /// </summary>
        private static int DefaultPageSize = Int32.MaxValue;

        /// <summary>
        /// For rate-limited requests, retry for a default of 10 times before failing the request.
        /// For infinite retries, set to -1.
        /// </summary>
        private static int MaxRetryOnRateLimiting = 10;

        /// <summary>
        /// Maximum number of connections that can be established to a single host.  
        /// Set this to a high number to allow load-balancing collections to multiple CosmosDB nodes.
        /// </summary>
        private static int MaxConnectionsPerHost = 300;

        /// <summary>
        /// Minimum number of connections that need to be established to a single host. 
        /// </summary>
        private static int CoreConnectionsPerHost = 1;

        /// <summary>
        /// Maximum number of requests that can be in flight for a single connection.
        /// For highly throughput scenario's, increase <see cref="MaxConnectionsPerHost"/> and <see cref="MaxRequestsPerConnection"/> together.
        /// </summary>
        private static int MaxRequestsPerConnection = 1024;

        /// <summary>
        /// Creates the <see cref="Cluster"/> object to connect to Azure CosmosDB Cassandra API
        /// </summary>
        /// <param name="hostName">Host name of the CosmosDB account, as shown in the "Connection String" section of Azure Portal</param>
        /// <param name="userName">UserName to connect to the CosmosDB account, as shown in the "Connection String" section of Azure Portal</param>
        /// <param name="password">
        /// Password to connect the CosmosDB account, as shown in the "Connection String" section of Azure Portal.
        /// You can use either the "Primary Master ReadWrite" or "Seconday Master ReadWrite" keys here.
        /// </param>
        public static Cluster CreateClusterForCosmosDB(string hostName, string userName, string password)
        {
            // Refer https://docs.datastax.com/en/developer/java-driver/2.1/manual/pooling/ for more details
            PoolingOptions poolingOptions = new PoolingOptions();
            poolingOptions.SetCoreConnectionsPerHost(HostDistance.Local, ClusterBuilderHelpers.CoreConnectionsPerHost);
            poolingOptions.SetMaxRequestsPerConnection(ClusterBuilderHelpers.MaxRequestsPerConnection);
            poolingOptions.SetMaxConnectionsPerHost(HostDistance.Local, ClusterBuilderHelpers.MaxConnectionsPerHost);

            // Increase the page size to Max to avoid multiple pagination calls.
            QueryOptions defaultOptions = new QueryOptions();
            defaultOptions.SetPageSize(ClusterBuilderHelpers.DefaultPageSize);

            // Set a custom host name resolver to ensure that SSL does not fail with RemoteCertificateNameMismatch
            // Ensure that we return the same hostname is passed so that it can be matched with the CNAME from the certificate.
            SSLOptions sslOptions = new SSLOptions(SslProtocols.Tls12, checkCertificateRevocation: true, remoteCertValidationCallback: ValidateServerCertificate);
            sslOptions.SetHostNameResolver((ipAddress) => hostName);

            return Cluster
                .Builder()
                .WithCredentials(userName, password)
                .WithPort(ClusterBuilderHelpers.CosmosDBCassandraPort)
                .WithQueryOptions(defaultOptions)
                .WithRetryPolicy(new LoggingRetryPolicy(new CosmosDBMultipleRetryPolicy(ClusterBuilderHelpers.MaxRetryOnRateLimiting)))
                .AddContactPoint(hostName)
                .WithSSL(sslOptions)
                .Build();
        }

        private static bool ValidateServerCertificate(
            object sender,
            X509Certificate certificate,
            X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            Console.WriteLine("Certificate error: {0}", sslPolicyErrors);
            // Do not allow this client to communicate with unauthenticated servers.
            return true;
        }
    }
}
