// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

namespace CassandraQuickstart.Scenarios
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Cassandra;

    internal sealed class BasicCRUDScenario : ScenarioBase
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

            // Insert some tracks, one record at a time
            double totalRUConsumed = 0;
            Console.WriteLine();
            Console.WriteLine($"Inserting records into the tracks table");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('2wYHz0GOHV49Xezd5mWTDP', 'Falling', 201385,'Falling','Lina Mayer', 'Pop')"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('5SxkdsY1ufZzoq9iXceLw9', 'No Tears Left To Cry', 205947,'No Tears Left To Cry','Ariana Grande', 'Pop')"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('2ARqIya5NAuvFVHSN3bL0m', 'The Middle', 184732,'The Middle','Zedd', 'Pop')"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('0BlY60NrN0fFWbdW3RW40q', 'Never Be the Same', 184732,'Never Be the Same ','Camila Cabello', 'Pop')"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement(
                "INSERT INTO music.tracks (id, title, durationms, album, artist, genre) VALUES ('6T8cJz5lAqGer9GUHGyelE', 'Gods Plan', 198960,'Scary Hours','Drake', 'Pop')"));
            totalRUConsumed += CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet);
            Console.WriteLine("Inserted 5 records. Consumed RU: {0:F2}", totalRUConsumed);

            // Read a single track
            string trackId = "6T8cJz5lAqGer9GUHGyelE";
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

            // Update a single track
            Console.WriteLine();
            Console.WriteLine($"Updating duration of track: '{trackId}'");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement($"UPDATE music.tracks SET durationms = 227077 WHERE id='{trackId}'"));
            Console.WriteLine(
                "Updated duration of track: {0}.  Consumed RU: {1:F2}",
                trackId,
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Read a single track
            Console.WriteLine();
            Console.WriteLine($"Reading track: '{trackId}'");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement($"SELECT * FROM music.tracks WHERE id='{trackId}'"));
            track = rowSet.GetRows().First();
            Console.WriteLine(
                "Read track: {0}, title: {1} duration: {2} from album: {3}. Consumed RU: {4:F2}",
                trackId,
                track.GetValue<string>("title"),
                track.GetValue<int>("durationms"),
                track.GetValue<string>("album"),
                CustomPayloadHelpers.ExtractRequestChargeFromCustomPayload(rowSet));

            // Delete a single track.
            Console.WriteLine();
            Console.WriteLine($"Deleting track: '{trackId}'");
            rowSet = await this.Session.ExecuteAsync(new SimpleStatement($"DELETE FROM music.tracks WHERE id='{trackId}'"));
            Console.WriteLine(
                "Deleted track: {0}. Consumed RU: {1:F2}",
                trackId,
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
