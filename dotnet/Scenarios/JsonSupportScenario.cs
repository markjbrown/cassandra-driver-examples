// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart.Scenarios
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Cassandra;

    internal sealed class JsonSupportScenario : ScenarioBase
    {
        /// <inheritdoc />
        public override async Task RunScenarioAsync()
        {
            // Create the "music" keyspace
            Console.WriteLine();
            Console.WriteLine("Creating keyspace: music");
            RowSet rowSet = await CreateKeyspaceHelper.CreateKeyspaceAsync(this.Session, "music");
            Console.WriteLine(
                "Created keyspace: music. Consumed RU: {0:F2}",
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Create the tables necessary for the music service
            Console.WriteLine();
            //await this.Session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS music.tracks"));
            Console.WriteLine($"Creating table: tracks with provisioned throughput: {ScenarioBase.DefaultThroughputForQuickstart}");
            rowSet = await CreateTableHelpers.CreateTableWithThroughputInCQLAsync(
                this.Session,
                "CREATE TABLE IF NOT EXISTS music.tracks (id text PRIMARY KEY, title text, durationms int, album text, artist text, genre text)",
                ScenarioBase.DefaultThroughputForQuickstart);
            Console.WriteLine(
                "Created table: tracks. Consumed RU: {0:F2}",
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Insert one track
            double totalRUConsumed = 0;
            Console.WriteLine();
            Console.WriteLine($"Inserting records into the tracks table");
            string msg =
                "INSERT INTO music.tracks JSON '{\"id\": \"2wYHz0GOHV49Xezd5mWTDP\", \"title\": \"Falling\", \"durationms\": 201385, \"album\": \"Falling\", \"artist\":\"Lina Mayer\", \"genre\": \"Pop\"}'";
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks JSON '{\"id\": \"2wYHz0GOHV49Xezd5mWTDP\", \"title\": \"Falling\", \"durationms\": 201385, \"album\": \"Falling\", \"artist\":\"Lina Mayer\", \"genre\": \"Pop\"}'"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            Console.WriteLine("Inserted 1 record. Consumed RU: {0:F2}", totalRUConsumed);

            // Read a single track
            string trackId = "2wYHz0GOHV49Xezd5mWTDP";
            Console.WriteLine();
            Console.WriteLine($"Reading track: '{trackId}'");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement($"SELECT JSON * FROM music.tracks WHERE id='{trackId}'"));
            Row track = rowSet.GetRows().First();
            Console.WriteLine(
                "Read track: {0}, json: {1}. Consumed RU: {2:F2}",
                trackId,
                track.GetValue<string>("[json]"),
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));
            
            // Drop the table
            Console.WriteLine();
            Console.WriteLine($"Droping table: tracks ");
            //rowSet = await this.Session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS music.tracks"));
            Console.WriteLine(
                "Dropped table: tracks. Consumed RU: {0:F2}",
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));
        }
    }
}
