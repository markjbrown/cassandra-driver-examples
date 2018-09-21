// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart.CosmosDBUtils
{
    using System;
    using System.Threading;
    using Cassandra;

    /// <summary>
    /// The retry policy performs growing/fixed back-offs for overloaded exceptions based on the max retries: 
    ///  1.If Max retries == -1, i.e., retry infinitely, then we follow a fixed back - off scheme of 5 seconds on each retry.
    ///  2.If Max retries != -1, and is any positive number n, then we follow a growing back - off scheme of(i * 1) seconds where 'i' is the i'th retry.
    /// If you'd like to modify the back-off intervals, please update GrowingBackOffTimeMs and FixedBackOffTimeMs accordingly.
    /// </summary>
    internal sealed class CosmosDBMultipleRetryPolicy : IExtendedRetryPolicy
    {
        private const int GrowingBackOffTimeMs = 1000;
        private const int FixedBackOffTimeMs = 5000;

        private readonly int maxRetryCount;

        public CosmosDBMultipleRetryPolicy(int maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
        }

        /// <inheritdoc />
        public RetryDecision OnReadTimeout(
            IStatement query, 
            ConsistencyLevel cl, 
            int requiredResponses, 
            int receivedResponses,
            bool dataRetrieved, 
            int nbRetry)
        {
            if (nbRetry < this.maxRetryCount)
            {
                return RetryDecision.Retry(cl, useCurrentHost: true);
            }

            return RetryDecision.Rethrow();
        }

        /// <inheritdoc />
        public RetryDecision OnWriteTimeout(
            IStatement query, 
            ConsistencyLevel cl, 
            string writeType, 
            int requiredAcks,
            int receivedAcks, 
            int nbRetry)
        {
            if (query != null && query.IsIdempotent == true)
            {
                return this.GenerateRetryDecision(nbRetry, cl);
            }

            return RetryDecision.Rethrow();
        }

        /// <inheritdoc />
        public RetryDecision OnUnavailable(
            IStatement query, 
            ConsistencyLevel cl, 
            int requiredReplica, 
            int aliveReplica, 
            int nbRetry)
        {
            return RetryDecision.Rethrow();
        }

        /// <inheritdoc />
        public RetryDecision OnRequestError(
            IStatement statement, 
            Configuration config, 
            Exception ex, 
            int nbRetry)
        {
            if (ex is OverloadedException ||
                (ex is OperationTimedOutException && statement.IsIdempotent.GetValueOrDefault(false)))
            {
                return this.GenerateRetryDecision(nbRetry, statement.ConsistencyLevel);
            }

            return RetryDecision.Rethrow();
        }

        private RetryDecision GenerateRetryDecision(int nbRetry, ConsistencyLevel? cl)
        {
            if (this.maxRetryCount == -1)
            {
                Thread.Sleep(CosmosDBMultipleRetryPolicy.FixedBackOffTimeMs);
                return RetryDecision.Retry(cl, useCurrentHost: true);
            }
            else if (nbRetry < this.maxRetryCount)
            {
                Thread.Sleep(CosmosDBMultipleRetryPolicy.GrowingBackOffTimeMs * nbRetry);
                return RetryDecision.Retry(cl, useCurrentHost: true);
            }

            return RetryDecision.Rethrow();
        }
    }
}
