using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
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
    }
}
