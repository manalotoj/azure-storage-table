using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Org.Data.Test
{
    /// <summary>
    /// Use to generate sample data
    /// </summary>
    public class TableDataGeneratorTests
    {
        #region Initialize
        private readonly string accountName;
        private readonly string accountKey;
        private readonly string tableName;
        private readonly IConfigurationRoot config;
        private readonly CloudStorageAccount storageAccount;
        private readonly CloudTable table;

        public TableDataGeneratorTests()
        {
            config = new ConfigurationBuilder()
             .AddJsonFile("appSettings.json")
             .Build();

            var section = config.GetSection("storageAccount");
            accountName = section["name"];
            accountKey = section["key"];
            tableName = section["table"];

            ValidateConfig();

            storageAccount = new CloudStorageAccount(
                new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                    accountName, accountKey), true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(tableName);
        }

        private void ValidateConfig()
        {
            Assert.NotEmpty(accountName);
            Assert.NotEmpty(accountKey);
            Assert.NotEmpty(tableName);
        }
        #endregion

        /// <summary>
        /// Uncomment [Fact] attribute to execute.
        /// </summary>
        /// <returns></returns>
        //[Fact]
        public async Task AddRecords()
        {
            List<string> partitionKeys = new List<string>();
            for (int i = 0; i < 5; i++)
            {
                partitionKeys.Add(i.ToString());
            }
            var generator = new TableDataGenerator(accountName, accountKey, tableName);
            await generator.AddRows(partitionKeys, 50000);
        }

        private class TableDataGenerator
        {
            private readonly string accountName;
            private readonly string accountKey;
            private readonly string tableName;

            public TableDataGenerator(string accountName, string accountKey, string tableName)
            {
                this.accountName = accountName;
                this.accountKey = accountKey;
                this.tableName = tableName;
            }

            public async Task AddRows(List<string> partitions, int numberOfRows)
            {
                CloudStorageAccount storageAccount = new CloudStorageAccount(
                    new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                        accountName, accountKey), true);

                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                CloudTable table = tableClient.GetTableReference(tableName);

                int batchSize = 100;
                foreach (var partition in partitions)
                {
                    int recordsAdded = 0;
                    while (recordsAdded < numberOfRows)
                    {
                        if (numberOfRows - recordsAdded > batchSize)
                        {
                            await AddBatch(table, partition, batchSize);
                            recordsAdded += batchSize;
                        }
                        else
                        {
                            await AddBatch(table, partition, numberOfRows - recordsAdded);
                            recordsAdded = numberOfRows;
                        }
                    }
                }
            }

            private async Task AddBatch(CloudTable table, string partitionKey, int recordCount)
            {
                var batchOps = new TableBatchOperation();
                for (var i = 0; i < recordCount; i++)
                {
                    batchOps.Add(TableOperation.Insert(new TableEntity(partitionKey, Guid.NewGuid().ToString())));
                }
                await table.ExecuteBatchAsync(batchOps);
            }
        }
    }
}
