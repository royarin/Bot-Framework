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
        static void Main(string[] args)
        {
            try
            {
                //Get all top level directories in the container i.e. folder per bot channel
                var blobDirectories = GetTopLevelDirectories().Result;

                foreach (var blobdir in blobDirectories)
                {
                    Console.WriteLine(blobdir.Uri);
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
            Console.WriteLine(directory.Uri);

            JArray transcriptList = new JArray();

            // Get all blobs in the current directory (also includes directories)
            var blobList = await directory.ListBlobsSegmentedAsync(false, BlobListingDetails.None, int.MaxValue, null, null, null);

            // Get the list of the all directories from the list of blobs. These are the blob delimeters that are same as Conversation Id.
            var subdir = blobList.Results.OfType<CloudBlobDirectory>().ToList();

            foreach (var sdir in subdir)
            {
                // Recursively look in these folders
                await CreateTranscript(sdir);
            }

            // Get blobs in the directory
            var fileBlobs = blobList.Results.OfType<CloudBlob>();
            if (fileBlobs.Count() == 0)
            {
                return;
            }

            // Check if there is already a transcript file
            var transcripts = fileBlobs.Cast<CloudBlob>().Where(blob => Path.GetExtension(blob.Uri.ToString()).Equals(".transcript")).ToList();
            // Skip if transcript file already exists
            if (transcripts.Count() > 0)
            {
                return;
            }

            // Get all ativity logs ordered by LastModified Timestamp
            var files = blobList.Results.Cast<CloudBlob>().Where(blob => !Path.GetExtension(blob.Uri.ToString()).Equals(".transcript")).ToList().OrderBy(blob => blob.Properties.LastModified);

            if (files.Count() > 0)
            {
                foreach (var file in files)
                {
                    Console.WriteLine(HttpUtility.UrlDecode(file.Uri.ToString()));
                    Console.WriteLine(file.Properties.LastModified);

                    // If the latest activity log is less than 5 mins old, there is possibility that this is still an active conversation
                    // We skip creating transcripts for such conversations
                    if (file.Properties.LastModified > DateTime.Now.AddMinutes(-5))
                    {
                        return;
                    }

                    var blob = directory.GetBlobReference(HttpUtility.UrlDecode(file.Uri.Segments[file.Uri.Segments.Length - 1]));

                    string blobContent;

                    // Read blob content
                    using (StreamReader reader = new StreamReader(await blob.OpenReadAsync()))
                    {
                        blobContent = reader.ReadToEnd();
                    }


                    //Console.WriteLine(blobContent);

                    JObject act = JObject.Parse(blobContent);

                    // Add activity content to transcript
                    transcriptList.Add(act);


                }
                //Console.WriteLine(transcriptList.ToString());
                var directoryName = HttpUtility.UrlDecode(files.FirstOrDefault().Uri.Segments[files.FirstOrDefault().Uri.Segments.Length - 2]);

                // Create transcript blo reference. Transcript blobs are created with same name as Conversation Id for context
                var blobReference = directory.GetBlockBlobReference($"{directoryName.Substring(0, directoryName.Length - 1)}.transcript");
                blobReference.Properties.ContentType = "application/json";

                using (var blobStream = await blobReference.OpenWriteAsync().ConfigureAwait(false))
                {
                    using (var jsonWriter = new JsonTextWriter(new StreamWriter(blobStream)))
                    {
                        _jsonSerializer.Serialize(jsonWriter, transcriptList);
                    }
                }

                await blobReference.SetMetadataAsync().ConfigureAwait(false);
            }
        }
    }
}
