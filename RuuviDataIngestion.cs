using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Hadoop.Avro.Container;
using Newtonsoft.Json;
using Npgsql;

namespace VirratHacklab.IoT
{
    public static class RuuviDataIngestion
    {
        [FunctionName("RuuviDataIngestion")]
        public static void Run([BlobTrigger("iot/virrat-hacklab-hub/virrat-hacklab-iot-ruuvi/{name}", Connection = "AzureWebJobsStorage")]Stream telemetry, string name, ILogger log)
        {

            List<AvroRuuviConditionsData> conditions = new List<AvroRuuviConditionsData>();

            using (var reader = AvroContainer.CreateGenericReader(telemetry))
            {
                log.LogInformation($"Processing file: {name}, Size: {telemetry.Length}");
                while (reader.MoveNext())
                {
                    log.LogInformation($"Processing results");
                    foreach (dynamic result in reader.Current.Objects)
                    {
                        var record = new AvroRuuviConditionsData(result);
                        log.LogInformation($"Adding {record} to list");
                        conditions.Add(record);
                    }
                }
            }
            log.LogInformation($"Initializing writer");
            RuuviDataWriter writer = new RuuviDataWriter();
            log.LogInformation($"Writing data to database");
            writer.insertRuuviConditionsData(conditions, log);
        }    
    }

    public class RuuviDataWriter
    {
        public void insertRuuviConditionsData(List<AvroRuuviConditionsData> records, ILogger log)
        {
            using (var connection = new NpgsqlConnection(Environment.GetEnvironmentVariable("PGSQL_CONNECTIONSTRING")))
            {
                connection.Open();
                log.LogInformation($"Connected to database");

                try
                {
                    foreach (var record in records)
                    {
                        var sequenceNumber = record.SequenceNumber;
                        var json = Encoding.UTF8.GetString(record.Body);
                        RuuviTelemetry ruuviTelemetry = JsonConvert.DeserializeObject<RuuviTelemetry>(json);

                        log.LogInformation($"Processing: {json}");

                        using (var command = connection.CreateCommand())
                        {
                            command.CommandText = "insert into ruuvi_telemetry " +
                                "(device_id, time, parameters) values " +
                                "((select id from device where address=@mac), @datetime, ROW(@temperature, @humidity, @pressure, @voltage, @txPower))";

                            command.Parameters.AddWithValue("@mac", ruuviTelemetry.device.address);
                            command.Parameters.AddWithValue("@datetime", ruuviTelemetry.datetime);
                            command.Parameters.AddWithValue("@temperature", ruuviTelemetry.sensors.temperature);
                            command.Parameters.AddWithValue("@humidity", ruuviTelemetry.sensors.humidity);
                            command.Parameters.AddWithValue("@pressure", ruuviTelemetry.sensors.pressure);
                            command.Parameters.AddWithValue("@voltage", ruuviTelemetry.sensors.voltage);
                            command.Parameters.AddWithValue("@txPower", ruuviTelemetry.sensors.txPower);

                            log.LogInformation($"Inserting: {command.ToString()}");

                            command.ExecuteNonQuery();
                        }
                    }
                }
                catch (Exception e)
                {
                    log.LogError("Failed: " + e.Message);
                    log.LogError(e.StackTrace);
                }
            }
        }
    }

    public struct AvroRuuviConditionsData
    {
        public AvroRuuviConditionsData(dynamic record)
        {
            SequenceNumber = (long)record.SequenceNumber;
            Offset = (string)record.Offset;
            DateTime.TryParse((string)record.EnqueuedTimeUtc, out var enqueuedTimeUtc);
            EnqueuedTimeUtc = enqueuedTimeUtc;
            SystemProperties = (Dictionary<string, object>)record.SystemProperties;
            Properties = (Dictionary<string, object>)record.Properties;
            Body = (byte[])record.Body;
        }
        public long SequenceNumber { get; set; }
        public string Offset { get; set; }
        public DateTime EnqueuedTimeUtc { get; set; }
        public Dictionary<string, object> SystemProperties { get; set; }
        public Dictionary<string, object> Properties { get; set; }
        public byte[] Body { get; set; }
    }
    /* {
     *      "device": {
     *          "address":"e4:f7:db:2b:8c:c9","type":"LE Random"
     *      },
     *      "rssi":-28,
     *      "sensors": {
     *          "humidity":21,
     *          "temperature":22.6,
     *          "pressure":99605,
     *          "accelerationX":-0.06,
     *          "accelerationY":-0.024,
     *          "accelerationZ":1.048,
     *          "voltage":2869,
     *          "txpower":31,
     *          "movementCount":255,
     *          "sequence":65535
     *      }
     * }*/
    public class RuuviDevice
    {
        public string address { get; set; }
        public string type { get; set; }
    }

    public class RuuviSensors
    {
        public double humidity { get; set; }
        public double temperature { get; set; }
        public int pressure { get; set; }
        public double accelerationX { get; set; }
        public double accelerationY { get; set; }
        public double accelerationZ { get; set; }
        public int txPower { get; set; }
        public int voltage { get; set; }
        public int movementCount { get; set; }
        public int sequence { get; set; }
    }

    public class RuuviTelemetry
    {
        public RuuviDevice device { get; set; }
        public int rssi { get; set; }
        public RuuviSensors sensors { get; set; }
        public DateTime datetime { get; set; }
    }
}
