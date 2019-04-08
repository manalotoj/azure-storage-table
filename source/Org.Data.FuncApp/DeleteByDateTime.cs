using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace Org.Data.FuncApp
{
    /// <summary>
    /// Durable function to delete entities by datetime offset.
    /// </summary>
    /// <remarks>
    /// Implemented as durable functions as this process may exceed Functions execution time of 10 minutes max.
    /// </remarks>
    public static class DeleteByDateTime
    {
        [FunctionName("DeleteByDateTime")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            var outputs = new List<string>();

            // Replace "hello" with the name of your Durable Activity Function.
            outputs.Add(await context.CallActivityAsync<string>("DeleteByDateTime_Hello", "Tokyo"));
            outputs.Add(await context.CallActivityAsync<string>("DeleteByDateTime_Hello", "Seattle"));
            outputs.Add(await context.CallActivityAsync<string>("DeleteByDateTime_Hello", "London"));

            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        [FunctionName("DeleteByDateTime_Hello")]
        public static string SayHello([ActivityTrigger] string name, ILogger log)
        {
            log.LogInformation($"Saying hello to {name}.");
            return $"Hello {name}!";
        }

        [FunctionName("DeleteByDateTime_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("DeleteByDateTime", null);

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}