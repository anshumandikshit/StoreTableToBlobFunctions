using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Linq;

namespace StoreTableToblobFunction
{
    public static class TakeLastefileToProcessed
    {
        [FunctionName("TakeLastefileToProcessed")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            //Connect the Blob storage and Download the latest modified blob from the storage and store it to another location 
            //Then all other blobstorage files to be stored in the Archive container 

            //Then delete all those files from "input" contaniner

            //Getting the Blob storage Account and BlobClient


            try
            {
                var cloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("blobstorageConnectionString"));

                var BlobClient = cloudStorageAccount.CreateCloudBlobClient();

                //Getting the Source container reference
                var sourceContainer = BlobClient.GetContainerReference("input");
                await sourceContainer.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Blob, null, null);

                var blobSegemnts = await sourceContainer.ListBlobsSegmentedAsync(null, null);

                var lastModifiedBlob = blobSegemnts.Results.OfType<CloudBlockBlob>().OrderByDescending(x => x.Properties.LastModified).FirstOrDefault();
                Console.WriteLine($"Last modified: {lastModifiedBlob.Properties.LastModified}");

                MemoryStream ms = new MemoryStream();
                await lastModifiedBlob.DownloadToStreamAsync(ms).ConfigureAwait(false);


                //getting the reference of destinationContainer
                var destinationContainer = BlobClient.GetContainerReference("processed");
                await destinationContainer.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Blob, null, null);

                CloudBlockBlob destinationBlob = destinationContainer.GetBlockBlobReference(lastModifiedBlob.Name);
                await destinationBlob.StartCopyAsync(new Uri(GetSharedAccessUri(lastModifiedBlob.Name, sourceContainer)));

                ICloudBlob destBlobRef = await destinationContainer.GetBlobReferenceFromServerAsync(lastModifiedBlob.Name);
                while (destBlobRef.CopyState.Status == CopyStatus.Pending)
                {
                    Console.WriteLine($"Blob: {destBlobRef.Name}, Copied: {destBlobRef.CopyState.BytesCopied ?? 0} of  {destBlobRef.CopyState.TotalBytes ?? 0}");
                    await Task.Delay(500);
                    destBlobRef = await destinationContainer.GetBlobReferenceFromServerAsync(destBlobRef.Name);
                }

                if (destinationBlob.CopyState.Status == CopyStatus.Success)
                {
                    //Delete the file from the Source container
                    bool blobExisted = await lastModifiedBlob.DeleteIfExistsAsync();
                }

            }
            catch (Exception ex)
            {
                return new OkObjectResult(ex.InnerException);
            }


            return new OkObjectResult("Checking the Blob Copying");
        }


        private static string GetSharedAccessUri(string blobName, CloudBlobContainer container)
        {
            DateTime toDateTime = DateTime.Now.AddMinutes(60);

            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = new DateTimeOffset(toDateTime)
            };

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string sas = blob.GetSharedAccessSignature(policy);

            return blob.Uri.AbsoluteUri + sas;
        }
    }
}
