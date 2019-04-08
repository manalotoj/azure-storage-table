using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Org.Data.Test
{
    internal class TableDataGenerator
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
