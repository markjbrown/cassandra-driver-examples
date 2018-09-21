// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart
{
    using System;
    using System.Threading.Tasks;
    using global::CassandraQuickstart.Scenarios;

    // https://www.kaggle.com/varadtupe/spotifytopplaylist#1000Tracks.csv
    class Program
    {
        private const string DefaultQuickstartScenario = "BasicCRUDScenario";

        static void Main(string[] args)
        {
            try
            {
                Program.MainAsync(args).Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Caught exception: {e}");
            }
        }

        static async Task MainAsync(string[] args)
        {
            string scenarioToRun = Program.DefaultQuickstartScenario;
            if (args.Length == 1)
            {
                scenarioToRun = args[0];
            }

            // Read the input parameters, either from command line or environment variables
            //string hostname = CassandraQuickstart.GetEnvironmentVariable("hostname", "<FILL_IN_FROM_AZURE_PORTAL>");
            //string userName = CassandraQuickstart.GetEnvironmentVariable("userName", "<FILL_IN_FROM_AZURE_PORTAL>");
            //string password = CassandraQuickstart.GetEnvironmentVariable("password", "<FILL_IN_FROM_AZURE_PORTAL>");
            string hostname = Program.GetEnvironmentVariable("hostname", "sivethetest-cassandra.cassandra.cosmosdb.azure.com");
            string userName = Program.GetEnvironmentVariable("user", "sivethetest-cassandra");
            string password = Program.GetEnvironmentVariable("password", "Nap6LRrHFAPydUqv6TWDXUXaaHXaw7fb68lsc6TNDq9xC0QlYQsDhiIiA6q0PzvXZ9JJ89FhlrVXqwTA0mcckQ==");

            Console.WriteLine($"Executing scenario: {scenarioToRun}\n");
            ScenarioBase scenario = Program.ConstructScenario(scenarioToRun);
            await scenario.InitializeAsync(hostname, userName, password);
            await scenario.RunScenarioAsync();
        }

        private static ScenarioBase ConstructScenario(string scenarioName)
        {
            switch (scenarioName)
            {
                case "BasicCRUDScenario":
                    return new BasicCRUDScenario();
                case "BasicCRUDPreparedScenario":
                    return new BasicCRUDPreparedScenario();
                case "TimeToLiveScenario":
                    return new TimeToLiveScenario();
            }

            throw new ArgumentException($"Invalid scenario: {scenarioName}");
        }

        private static string GetEnvironmentVariable(string parameterName, string defaultValue)
        {
            string value = Environment.GetEnvironmentVariable(parameterName);
            return string.IsNullOrEmpty(value) ? defaultValue : value;
        }
    }
}
