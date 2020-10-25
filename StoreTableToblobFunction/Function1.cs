using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Azure.Storage.Blobs;

using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System.Text;
using System.Text.RegularExpressions;

namespace StoreTableToblobFunction
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            var connectionString = Environment.GetEnvironmentVariable("sqlConnection");
            bool successful = false;
            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))

                {
                    
                    var currentTime = Regex.Replace(DateTime.Now.ToString(), @"[^\w\d]", "");
                    using (SqlCommand cmd = new SqlCommand())

                    {

                        SqlDataReader dataReader;
                        cmd.CommandText = "Select * from [dbo].[Persons]";
                        cmd.CommandType = CommandType.Text;
                        cmd.Connection = conn;
                        conn.Open();
                        dataReader = cmd.ExecuteReader();

                        //var r = Serialize(dataReader);

                        //json = JsonConvert.SerializeObject(r, Formatting.Indented);
                        //Convert it to CSV
                        //Write the file to BlobStorage by creating the connections

                        //CreateBlobConnection();
                        var dataToCSVString = ToCsv(dataReader, "Test2", "");
                        var fileName = $"DataTable{currentTime}.csv";
                        var blobContainer = await GetContainerReference("input");
                        CloudBlockBlob cloudBlob = blobContainer.GetBlockBlobReference(fileName);

                        Console.WriteLine($"fileName: {fileName}");
                        log.LogInformation($"fileName: {fileName}");

                        using MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(dataToCSVString));
                        await cloudBlob.UploadFromStreamAsync(ms);
                        successful = true;
                        //StringBuilder csvData = new StringBuilder();
                        //csvData.AppendLine("00,01,02,03,04,05,06,07,08,09");
                        //csvData.AppendLine("10,11,12,13,14,15,16,17,18,19");
                        //csvData.AppendLine("20,21,22,23,24,25,26,27,28,29");

                        //string dataUpload = csvData.ToString();

                        //var dataUploadArray = Encoding.UTF8.GetBytes(dataUpload);
                        //using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(dataUpload)))
                        //{
                        //   await cloudBlob.UploadFromStreamAsync(ms);
                        //}

                        //await cloudBlob.OpenWriteAsync();

                    }

                }
            }
            catch (Exception ex)
            {
                log.LogError($"Please read the log for exception :{ex.InnerException}");
                successful = false;
            }

            return !successful ? new OkObjectResult("Something went wrong .Please check the logs for more info") : new OkObjectResult("CSV file uploaded to Blob");
        }

        public static string ToCsv(this IDataReader reader, string filename, string path = null, string extension = "csv")
        {
            StringBuilder csvData = new StringBuilder();
            string dataToCSV = string.Empty;
            do
            {
                var filePath = Path.Combine(string.IsNullOrEmpty(path) ? Path.GetTempPath() : path, string.Format("{0}.{1}", filename, extension));

                csvData.AppendLine(string.Join(",", Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToList()));
                while (reader.Read())
                {
                    csvData.AppendLine(string.Join(",", Enumerable.Range(0, reader.FieldCount).Select(reader.GetValue).ToList()));
                };

            }
            while (reader.NextResult());

            return csvData.ToString();
        }


        private static async Task<CloudBlobContainer> GetContainerReference(string containerName)
        {

            var cloudStorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("blobstorageConnectionString"));

            var cloudBlobClient = cloudStorageAccount.CreateCloudBlobClient();

            var cloudBlobContainer = cloudBlobClient.GetContainerReference(containerName);
            await cloudBlobContainer.CreateIfNotExistsAsync(
              BlobContainerPublicAccessType.Blob, null, null);
            return cloudBlobContainer;
        }

    }


}


