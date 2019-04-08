using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Org.Data.Utilities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Org.Data.Test
{
    /// <summary>
    /// *****************
    /// **** WARNING **** These suite of tests will delete all data within the configured Azure Storage table.
    /// *****************
    /// </summary>
    public class TableHelperTests
    {
        #region Initialize
        private readonly string accountName;
        private readonly string accountKey;
        private readonly string tableName;
        private readonly IConfigurationRoot config;
        private readonly CloudStorageAccount storageAccount;
        private readonly CloudTable v1Table;
        private TableHelper helper;

        public TableHelperTests()
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
            v1Table = tableClient.GetTableReference(tableName);

            helper = new TableHelper(accountName, accountKey, tableName);
            helper.Delete(DateTime.Now).GetAwaiter().GetResult();
        }

        private void ValidateConfig()
        {
            Assert.NotEmpty(accountName);
            Assert.NotEmpty(accountKey);
            Assert.NotEmpty(tableName);
        } 
        #endregion

        [Fact]
        private void Delete()
        {
            helper.Delete(DateTime.Now).GetAwaiter().GetResult();
        }    

        [Fact]
        public async Task GetRecordCount_Returns_0_WhenThereAreNoRecords()
        {
            Assert.Equal(0, await helper.GetRecordCount(DateTime.Now));
        }

        [Fact]
        public async Task GetRecordCount_Returns_ExpectedNumber_Of_Records_Older_Than_Given_DateTime()
        {
            // arrange
            DateTime offset = default(DateTime);            
            for (int i = 0; i < 10; i++)
            {
                await v1Table.ExecuteAsync(TableOperation.Insert(new TableEntity(Guid.NewGuid().ToString(), Guid.NewGuid().ToString())));
                Thread.Sleep(1000);
                if (i == 3)
                {
                    offset = DateTime.Now;
                }
            }

            // act
            int result = await helper.GetRecordCount(offset);

            // assert
            Assert.Equal(4, result);

            // clean up
            await helper.Delete(DateTime.Now);
        }

        [Fact]
        public async Task DeleteOldRecords()
        {
            // arrange
            DateTime offset = default(DateTime);
            for (int i = 0; i < 10; i++)
            {
                await v1Table.ExecuteAsync(TableOperation.Insert(new TableEntity(Guid.NewGuid().ToString(), Guid.NewGuid().ToString())));
                Thread.Sleep(1000);
                if (i == 3)
                {
                    offset = DateTime.Now;
                }
            }

            Assert.Equal(10, await helper.GetRecordCount(DateTime.Now));

            // act
            await helper.Delete(offset);

            Assert.Equal(6, await helper.GetRecordCount(DateTime.Now));
            await helper.Delete(DateTime.Now);
        }

        [Fact]
        public async Task DeleteOldRecordsForPartition()
        {
            string partitionKey = Guid.NewGuid().ToString();
            DateTime offset = default(DateTime);
            // arrange
            for (int i = 0; i < 10; i++)
            {
                await v1Table.ExecuteAsync(TableOperation.Insert(new TableEntity(partitionKey, Guid.NewGuid().ToString())));
                Thread.Sleep(1000);
                if (i == 3)
                {
                    offset = DateTime.Now;
                }                
            }

            Assert.Equal(10, await helper.GetRecordCount(DateTime.Now, partitionKey));

            // act
            await helper.Delete(offset, partitionKey);

            Assert.Equal(6, await helper.GetRecordCount(DateTime.Now, partitionKey));
        }

    }
}
