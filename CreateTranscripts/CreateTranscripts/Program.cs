using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace CreateTranscripts
{
    class Program
    {
        private static readonly JsonSerializer _jsonSerializer = JsonSerializer.Create(new JsonSerializerSettings()
        {
            NullValueHandling = NullValueHandling.Ignore,
            Formatting = Formatting.Indented,
        });

        private static string storageConnection = ConfigurationManager.AppSettings["StorageConnectionString"];
        private static CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(storageConnection);
        private static CloudBlobClient client = cloudStorageAccount.CreateCloudBlobClient();
        private static CloudBlobContainer sContainer = client.GetContainerReference(ConfigurationManager.AppSettings["BlobContainerName"]);

        private static string conversationId = string.Empty;

        static void Main(string[] args)
        {
            try
            {
                do
                {
                    Console.Write("Enter the conversationId to generate the transcript: ");
                    var convId = Console.ReadLine();
                    conversationId = convId.Trim();
                } while (string.IsNullOrEmpty(conversationId));

                //Get all top level directories in the container i.e. folder per bot channel
                var blobDirectories = GetTopLevelDirectories().Result;

                foreach (var blobdir in blobDirectories)
                {
                    // Create transcripts per channel
                    CreateTranscript(blobdir).Wait();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"There was an exception: {ex.ToString()}");
            }
        }

        /// <summary>
        /// Function to get all directories in the container
        /// </summary>
        /// <returns></returns>
        public static async Task<List<CloudBlobDirectory>> GetTopLevelDirectories()
        {
            var blobList = await sContainer.ListBlobsSegmentedAsync(string.Empty, false, BlobListingDetails.None, int.MaxValue, null, null, null);

            return blobList.Results.OfType<CloudBlobDirectory>().ToList();
        }

        /// <summary>
        /// Create Transcripts per conversation by aggregating activity logs
        /// </summary>
        /// <param name="directory"></param>
        /// <returns></returns>
        public static async Task CreateTranscript(CloudBlobDirectory directory)
        {
            JArray transcriptList = new JArray();

            var channelId = HttpUtility.UrlDecode(directory.Uri.Segments[directory.Uri.Segments.Length - 1]);
            if (channelId.EndsWith("/"))
            {
                channelId = channelId.Substring(0, channelId.Length - 1);
            }

            var dirName = GetDirName(channelId, conversationId);
            var dir = sContainer.GetDirectoryReference(dirName);

            // Get all blobs in the current directory (also includes directories)
            var blobList = await dir.ListBlobsSegmentedAsync(false, BlobListingDetails.None, int.MaxValue, null, null, null);

            // Get all activity blobs
            var activities = blobList.Results.Cast<CloudBlob>().Where(blob => Path.GetExtension(blob.Uri.ToString()).Equals(".json")).ToList().OrderBy(blob => blob.Properties.LastModified);

            //Check if there are activities
            if (activities.Count() > 0)
            {
                foreach (var activity in activities)
                {
                    string blobContent;

                    // Read blob content
                    using (StreamReader reader = new StreamReader(await activity.OpenReadAsync()))
                    {
                        blobContent = reader.ReadToEnd();
                    }

                    JObject act = JObject.Parse(blobContent);

                    // Add activity content to transcript
                    transcriptList.Add(act);
                }

                // Verify and create transcript output path if not present
                var transcriptPath = $"{Environment.CurrentDirectory}/MyTranscripts";
                if (!Directory.Exists(transcriptPath))
                {
                    Directory.CreateDirectory(transcriptPath);
                }
                
                // Write transcript to file in transcript path
                using (StreamWriter file = File.CreateText($"{transcriptPath}/{SanitizeKey(conversationId)}.transcript"))
                using (JsonTextWriter writer = new JsonTextWriter(file))
                {
                    transcriptList.WriteTo(writer);
                }
            }
        }

        private static string GetDirName(string channelId, string conversationId)
        {
            string dirName = string.Empty;

            var convId = SanitizeKey(conversationId);
            NameValidator.ValidateDirectoryName(channelId.ToString());
            NameValidator.ValidateDirectoryName(convId);
            dirName = $"{channelId}/{convId}";

            return dirName;
        }

        private static string SanitizeKey(string key)
        {
            // Blob Name rules: case-sensitive any url char
            return Uri.EscapeDataString(key);
        }
    }
}
