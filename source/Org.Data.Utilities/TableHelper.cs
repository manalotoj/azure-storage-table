using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Org.Data.Utilities
{
    /// <summary>
    /// An example of querying/deleting table entities by date, specifically, Timestamp and, optionally, partition key value.
    /// </summary>
    /// <remarks>
    /// Note that Timestamp is system generated and is not indexed. As such, underlying queries will result in table scans. Consider providing partition key value to narrow query criteria.
    /// </remarks>
    public class TableHelper
    {
        private const string TIMESTAMP = "Timestamp";
        private const string PARTITION_KEY = "PartitionKey";
        private readonly string accountName;
        private readonly string accountKey;
        private readonly string tableName;

        public TableHelper(string accountName, string accountKey, string tableName)
        {
            this.accountName = accountName;
            this.accountKey = accountKey;
            this.tableName = tableName;
        }

        /// <summary>
        /// Retrieve number of records older than a given date.
        /// </summary>
        /// <param name="date">A <see cref="Datetime"/> value</param>
        /// <param name="partitionKey">optional partition</param>
        /// <returns>number of entities older than <paramref name="date"/></returns>
        public async Task<int> GetRecordCount(DateTime date, string partitionKey = null)
        {
            CloudStorageAccount storageAccount = new CloudStorageAccount(
    new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
        accountName, accountKey), true);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);
           
            TableContinuationToken token = null;
            var filter = TableQuery.GenerateFilterConditionForDate(TIMESTAMP, QueryComparisons.LessThan, date);
            if (partitionKey != null)
            {
                filter = TableQuery.CombineFilters(filter,
                    TableOperators.And, TableQuery.GenerateFilterCondition(PARTITION_KEY, QueryComparisons.Equal, partitionKey));
            }
            var query = new TableQuery<TableEntity>().Where(filter);

            var querySegment = await table.ExecuteQuerySegmentedAsync(query, token);
            int recordCount = querySegment.Results.Count;

            while (querySegment.ContinuationToken != null)
            {
                querySegment = await table.ExecuteQuerySegmentedAsync(query, querySegment.ContinuationToken);
                recordCount += querySegment.Results.Count;
            }
            return recordCount;
        }

        /// <summary>
        /// Delete records older than a given <see cref="DateTime"/>
        /// </summary>
        /// <param name="date">A <see cref="Datetime"/> value</param>
        /// <param name="partitionKey">optional partition</param>
        /// 
        /// <returns></returns>
        public async Task Delete(DateTime date, string partitionKey = null)
        {
            CloudStorageAccount storageAccount = new CloudStorageAccount(
                new Microsoft.WindowsAzure.Storage.Auth.StorageCredentials(
                    accountName, accountKey), true);

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            TableContinuationToken token = null;
            var filter = TableQuery.GenerateFilterConditionForDate(TIMESTAMP, QueryComparisons.LessThan, date);
            if (partitionKey != null)
            {
                filter = TableQuery.CombineFilters(filter,
                    TableOperators.And, TableQuery.GenerateFilterCondition(PARTITION_KEY, QueryComparisons.Equal, partitionKey));
            }

            var query = new TableQuery<TableEntity>().Where(filter);

            var querySegment = await table.ExecuteQuerySegmentedAsync(query, token);
            Task.WaitAll(DeleteBatch(table, querySegment).ToArray());

            while (querySegment.ContinuationToken != null)
            {
                querySegment = await table.ExecuteQuerySegmentedAsync(query, querySegment.ContinuationToken);

                // parallel execution of up to 10 batches of 100 rows
                Task.WaitAll(DeleteBatch(table, querySegment).ToArray());
            }
        }

        /// <summary>
        /// Delete entities in batches
        /// </summary>
        /// <param name="table">instance of <see cref="CloudTable"/></param>
        /// <param name="segment">instance of <see cref="TableQuerySegment"/></param>
        /// <returns>List of batch execution tasks</returns>
        private List<Task> DeleteBatch(CloudTable table, TableQuerySegment<TableEntity> segment)
        {
            if (segment.Count() == 0) return new List<Task>();

            // sort entities by partition key
            var list = segment.Results.GroupBy(x => x.PartitionKey);
            int batchSize = 100;
            List<Task> tasks = new List<Task>();

            // delete in batches by partition key
            foreach (var group in list)
            {
                int batchCount = 0;
                while (true)
                {
                    var items = group.Skip(batchCount * batchSize).Take(batchSize);
                    if (items.Count() == 0) break;

                    var batchOps = new TableBatchOperation();

                    foreach (var item in items)
                    {
                        batchOps.Delete(item);
                    }

                    Trace.WriteLine($"deleting {items.Count()} rows");
                    tasks.Add(table.ExecuteBatchAsync(batchOps));
                    batchCount++;
                }
            }
            return tasks;
        }
    }
}
