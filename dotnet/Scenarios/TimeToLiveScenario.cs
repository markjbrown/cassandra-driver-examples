// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart.Scenarios
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Cassandra;

    /// <inheritdoc />
    internal sealed class TimeToLiveScenario : ScenarioBase
    {
        private const int DefaultTableTTL = 10;

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

            // Create the tables necessary for the music service, with default TTL value of 10 seconds
            Console.WriteLine();
            //await this.Session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS music.tracks"));
            Console.WriteLine($"Creating table: tracks with provisioned throughput: {ScenarioBase.DefaultThroughputForQuickstart} and default TTL: {TimeToLiveScenario.DefaultTableTTL}");
            rowSet = await CreateTableHelpers.CreateTableWithThroughputInCQLAsync(
                this.Session,
                $"CREATE TABLE IF NOT EXISTS music.tracks (id text PRIMARY KEY, title text, durationms int, album text, artist text, genre text)",
                ScenarioBase.DefaultThroughputForQuickstart,
                TimeToLiveScenario.DefaultTableTTL);
            Console.WriteLine(
                "Created table: tracks. Consumed RU: {0:F2}",
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Insert one track using the default TTL
            double totalRUConsumed = 0;
            Console.WriteLine();
            Console.WriteLine($"Inserting record into the tracks table (default TTL)");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('2wYHz0GOHV49Xezd5mWTDP', 'Falling', 201385,'Falling','Lina Mayer', 'Pop')"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            Console.WriteLine("Inserted 1 record. Consumed RU: {0:F2}", totalRUConsumed);

            // Delay the execution to wait for TTL expiry
            Console.Write($"Delaying execution for {TimeToLiveScenario.DefaultTableTTL} seconds");
            await Task.Delay(TimeSpan.FromSeconds(TimeToLiveScenario.DefaultTableTTL*2));

            // Read a single track
            string trackId = "2wYHz0GOHV49Xezd5mWTDP";
            Console.WriteLine();
            Console.WriteLine($"Reading track: '{trackId}'");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement($"SELECT * FROM music.tracks WHERE id='{trackId}'"));
            int trackCount = rowSet.GetRows().Count();
            if (trackCount != 0)
            {
                throw new InvalidOperationException($"Expected no rows to be returned.  Service returned {trackCount} rows");
            }

            Console.WriteLine(
                "Read 0 rows from the system. Consumed RU: {0:F2}",
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Insert one track using the custom TTL
            int customRecordTTL = 20;
            Console.WriteLine();
            Console.WriteLine($"Inserting record into the tracks table (custom TTL: {customRecordTTL} seconds)");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                $"INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('2wYHz0GOHV49Xezd5mWTDP', 'Falling', 201385,'Falling','Lina Mayer', 'Pop') USING TTL {customRecordTTL}"));
            Console.WriteLine("Inserted 1 record. Consumed RU: {0:F2}", CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Delay the execution to wait for TTL expiry
            Console.WriteLine($"Delaying execution for {TimeToLiveScenario.DefaultTableTTL} seconds");
            await Task.Delay(TimeSpan.FromSeconds(TimeToLiveScenario.DefaultTableTTL));

            // Read a single track.  Record should exist as custom ttl is greater than default ttl
            Console.WriteLine();
            Console.WriteLine($"Reading track: '{trackId}'");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement($"SELECT * FROM music.tracks WHERE id='{trackId}'"));
            Row track = rowSet.GetRows().First();
            Console.WriteLine(
                "Read track: {0}, title: {1} duration: {2} from album: {3}. Consumed RU: {4:F2}",
                trackId,
                track.GetValue<string>("title"),
                track.GetValue<int>("durationms"),
                track.GetValue<string>("album"),
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Drop the table
            Console.WriteLine();
            Console.WriteLine($"Droping table: tracks ");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS music.tracks"));
            Console.WriteLine(
                "Dropped table: tracks. Consumed RU: {0:F2}",
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));
        }
    }
}
