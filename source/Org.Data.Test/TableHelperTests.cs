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

        [Theory]
        [InlineData(10, 4)]
        [InlineData(120, 33)]
        [InlineData(1005, 210)]
        public async Task GetRecordCount_Returns_ExpectedNumber_Of_Records_Older_Than_Given_DateTime(int recordsToInsert, int oldRecordCount)
        {
            // arrange
            List<string> partitionKeys = new List<string>();
            for (int i = 0; i < oldRecordCount; i++)
            {
                partitionKeys.Add(i.ToString());
            }
            TableDataGenerator generator = new TableDataGenerator(accountName, accountKey, tableName);

            await generator.AddRows(partitionKeys, 1);
            Thread.Sleep(1000);
            DateTime offset = DateTime.Now;

            partitionKeys.Clear();
            for (int i = oldRecordCount; i < recordsToInsert; i++)
            {
                partitionKeys.Add(i.ToString());
            }
            await generator.AddRows(partitionKeys, 1);

            Assert.Equal(oldRecordCount, await helper.GetRecordCount(offset));
        }

        [Theory]
        [InlineData(10, 4)]
        [InlineData(100, 60)]
        [InlineData(1000, 432)]
        public async Task DeleteOldRecordsWithoutPartition(int recordsToInsert, int recordsToDelete)
        {
            // arrange
            List<string> partitionKeys = new List<string>();
            for (int i = 0; i < recordsToDelete; i++)
            {
                partitionKeys.Add(i.ToString());
            }
            TableDataGenerator generator = new TableDataGenerator(accountName, accountKey, tableName);

            await generator.AddRows(partitionKeys, 1);
            Thread.Sleep(1000);
            DateTime offset = DateTime.Now;

            partitionKeys.Clear();
            for (int i = recordsToDelete; i < recordsToInsert; i++)
            {
                partitionKeys.Add(i.ToString());
            }
            await generator.AddRows(partitionKeys, 1);

            Assert.Equal(recordsToInsert, await helper.GetRecordCount(DateTime.Now));

            // act
            await helper.Delete(offset);

            Assert.Equal(recordsToInsert-recordsToDelete, await helper.GetRecordCount(DateTime.Now));
            await helper.Delete(DateTime.Now);
        }

        [Theory]
        [InlineData(10,4)]
        [InlineData(100, 60)]
        [InlineData(1000, 432)]
        //[InlineData(5500, 3700)]
        public async Task DeleteOldRecordsForPartition(int recordsToInsert, int recordsToDelete)
        {
            string partitionKey = Guid.NewGuid().ToString();
            // arrange
            TableDataGenerator generator = new TableDataGenerator(accountName, accountKey, tableName);

            await generator.AddRows(new List<string> { partitionKey }, recordsToDelete);
            Thread.Sleep(1000);
            DateTime offset = DateTime.Now;
            await generator.AddRows(new List<string> { partitionKey }, recordsToInsert-recordsToDelete);

            Assert.Equal(recordsToInsert, await helper.GetRecordCount(DateTime.Now, partitionKey));

            // act
            await helper.Delete(offset, partitionKey);

            Assert.Equal((recordsToInsert-recordsToDelete), await helper.GetRecordCount(DateTime.Now, partitionKey));
        }

    }
}
