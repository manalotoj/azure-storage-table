# Introduction 
The purpose of this repo is to provide an example of how to delete Azure Storage table entities older than a given date/time.

The code is in C#, .NET Core and authored using Visual Studio 2017. The solution contains the following projects:
- Org.Data.Utilities: a class library with a single class, TableHelper.cs. TableHelper supports operations to delete table entities older than a given date/time value. A partition key value can optionally be provided.
- Org.Data.Test: contains integration tests. WARNING: use an empty Azure Storage Table for testing as the tests will delete any/all existing entities within the specified table.
- Org.Data.ConsoleApp: a console application that references Org.Data.Utilities.
- Org.Data.FuncApp: placeholder for a Durable Function implementation.

Note that the underlying query to determine entity count or entities to be deleted are based on the entity Timestamp field. Timestamp is:
- generated by the system automatically. 
- updated each time an entity is updated.
- NOT indexed. It is best to provide partition key value if at all possible.
This can be changed to any date/time field. 

# Prerequisites
1. Visual Studio 2017 with the latest updates
2. Azure Storage Account and Storage table for integration testing
3. Clone repository locally, navigate to source folder, open and build solution using Visual Studio 2017 or later.

# Executing Org.Data.ConsoleApp
1. Use a command prompt and navigate to [root folder]\source\Org.Data.Consoleapp\bin\Debug\netcoreapp2.1
2. Run the following command to view parameters: dotnet Org.Data.ConsoleApp.dll --help
3. Run the following to retrieve a count of records older than a date: 
dotnet Org.Data.ConsoleApp.dll -cmd get -name [account-name] -key [account-key] -table [table-name] -offset "2019-04-08T12:20:00"
4. Run the following to delete the same records from step 3:
dotnet Org.Data.ConsoleApp.dll -cmd delete -name [account-name] -key [account-key] -table [table-name] -offset "2019-04-08T12:20:00"

# TODO
- Provide a Durable Functions implementation.
- Create a maximum number of records setting to limit execution duration. Millions of records will take a long time to process.
- Add reference to Azure Storage Table patterns and practices - https://docs.microsoft.com/en-us/azure/storage/tables/table-storage-design-patterns#intra-partition-secondary-index-pattern
