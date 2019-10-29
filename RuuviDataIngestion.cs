using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Hadoop.Avro.Container;

namespace VirratHacklab.IoT
{
    public static class RuuviDataIngestion
    {
        [FunctionName("RuuviDataIngestion")]
        public static void Run([BlobTrigger("iot/virrat-hacklab-hub/virrat-hacklab-iot-ruuvi/{name}", Connection = "AzureWebJobsStorage")]Stream telemetry, string name, ILogger log)
        {
            using (var reader = AvroContainer.CreateGenericReader(telemetry))
            {
                while (reader.MoveNext())
                {
                    foreach (dynamic result in reader.Current.Objects)
                    {
                        var record = new AvroEventData(result);
                        var sequenceNumber = record.SequenceNumber;
                        var bodyText = Encoding.UTF8.GetString(record.Body);
                        Console.WriteLine($"{sequenceNumber}: {bodyText}");
                        log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {telemetry.Length} Bytes \n body: {bodyText}");
                    }
                }
            }

        }
    }
    public struct AvroEventData
    {
        public AvroEventData(dynamic record)
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

}
