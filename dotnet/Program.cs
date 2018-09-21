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
            string hostname = Program.GetEnvironmentVariable("hostname", "cass-stage-signoff-newstack.cassandra.cosmosdb.windows-ppe.net");
            string userName = Program.GetEnvironmentVariable("user", "cass-stage-signoff-newstack");
            string password = Program.GetEnvironmentVariable("password", "QxfPRqlwRDZ2POgTkl8dR6C1n66b8twIPqCPCqiVAfR9jpZCSL8D0Y1n2FtCYBO4DEsMXyJ8DPOnGIWp7of2og==");

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
