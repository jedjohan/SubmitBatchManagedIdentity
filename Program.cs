using Azure;
using Azure.Core;
using Azure.Data.Tables;
using Azure.Identity;

namespace SubmitBatchManagedIdentity
{
    class Program
    {
        private static TableClient TableClient;
        private const string stringValue = "This is a string";
        private const string PartitionKey = "performance";
        private const string TableName = "perftest";
        private const string StorageUri = "https://[nameofstorageaccount].table.core.windows.net";
        private static readonly string UAIClientId = "GUID to User Assigned Identity";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Creating TableClient");

            TokenCredential tokenCredential = string.IsNullOrEmpty(Environment.GetEnvironmentVariable("WEBSITE_INSTANCE_ID")) // Check if running local
                ? new DefaultAzureCredential()
                : new ManagedIdentityCredential(UAIClientId);

            TableClient = new TableClient(new Uri(StorageUri), TableName, tokenCredential);

            var entities = GenerateEntities<SimplePerfEntity>(21);

            try
            {
                var addEntitiesBatch = new List<TableTransactionAction>(100);
                foreach (var entity in entities)
                {
                    addEntitiesBatch.Add(new TableTransactionAction(TableTransactionActionType.Add, entity));
                }

                var response = await TableClient.SubmitTransactionAsync(addEntitiesBatch).ConfigureAwait(false);
            }
            catch (Exception)
            {
                throw;
            }
        }

        private static IEnumerable<T> GenerateEntities<T>(int count) where T : class, ITableEntity, new()
        {
            return (IEnumerable<T>)Enumerable.Range(1, count)
                .Select(n =>
            {
                string number = n.ToString();
                return new SimplePerfEntity
                {
                    PartitionKey = PartitionKey,
                    RowKey = Guid.NewGuid().ToString(),
                    StringTypeProperty1 = stringValue,
                    StringTypeProperty2 = stringValue,
                    StringTypeProperty3 = stringValue,
                    StringTypeProperty4 = stringValue,
                    StringTypeProperty5 = stringValue,
                    DoubleTypeProperty = n * 0.3,
                    DateTimeTypeProperty = DateTime.Now,
                };
            });
        }
    }

    public record SimplePerfEntity : ITableEntity
    {
        public string StringTypeProperty1 { get; set; }
        public string StringTypeProperty2 { get; set; }
        public string StringTypeProperty3 { get; set; }
        public string StringTypeProperty4 { get; set; }
        public string StringTypeProperty5 { get; set; }
        public double DoubleTypeProperty { get; set; }
        public DateTime DateTimeTypeProperty { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }
    }
}