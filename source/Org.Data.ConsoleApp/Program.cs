using McMaster.Extensions.CommandLineUtils;
using Org.Data.Utilities;
using System;
using System.ComponentModel.DataAnnotations;

namespace Org.Data.ConsoleApp
{
    class Program
    {
        /// <summary>
        /// 
        /// </summary>
        public static int Main(string[] args)
            => CommandLineApplication.Execute<Program>(args);

        [Required]
        [Option(ShortName = "cmd", Description = "Command to execute. Allowed values include 'get' and 'delete'.")]
        public string Command { get; }

        [Required]
        [Option(ShortName = "name", Description = "Storage acccount name")]
        public string AccountName { get; }

        [Required]
        [Option(ShortName = "key", Description = "Account key")]
        public string Key { get; }

        [Required]
        [Option(ShortName = "table", Description = "Table name")]
        public string Table { get; }

        [Required]
        [Option(ShortName = "offset", Description = "Offset DateTime")]
        public string OffsetDate { get; }


        [Option(ShortName = "partition", Description = "Partition key value")]
        public string PartitionKey { get; }


        private void OnExecute()
        {
            Console.WriteLine($"Account name: {AccountName}");
            Console.WriteLine($"Offset date/time: {OffsetDate}");
            Console.WriteLine($"Table name: {Table}");
            Console.WriteLine($"Partition key value: {PartitionKey}");
            Console.WriteLine($"Command: {Command}");

            var helper = new TableHelper(AccountName, Key, Table);

            switch(Command.ToLower())
            {
                case "delete":
                    helper.Delete(DateTime.Parse(OffsetDate), PartitionKey).GetAwaiter().GetResult();
                    Console.WriteLine("delete command completed.");
                    break;
                case "get":
                    Console.Write($"number of records: {helper.GetRecordCount(DateTime.Parse(OffsetDate), PartitionKey).GetAwaiter().GetResult()}");
                    break;
                default:
                    Console.WriteLine($"Unrecognized command '{Command}' detected.");
                    break;
            }
        }
    }
}
