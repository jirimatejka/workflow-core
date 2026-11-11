using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Primitives;

namespace WorkflowCore.Services
{
    internal static class NestedWorkflowHelper
    {
        public static async Task PublishCompletion(IPersistenceProvider store, IQueueProvider queue, IDateTimeProvider clock, WorkflowInstance wf)
        {
            if (wf.Reference == null || !wf.Reference.StartsWith(RunWorkflow.ReferencePrefix))
                return;

            var eventId = await store.CreateEvent(new Event
            {
                EventName = Event.EventTypeWorkflowCompleted,
                EventKey = wf.Id,
                EventTime = clock.UtcNow,
                EventData = new WorkflowCompletedData
                {
                    WorkflowId = wf.Id,
                    Status = wf.Status,
                    Data = wf.Data
                },
                IsProcessed = false
            });
            await queue.QueueWork(eventId, QueueType.Event);
        }

        public static async Task TerminateChildren(IWorkflowController controller, IPersistenceProvider store, ILogger logger, IEnumerable<string> childIds)
        {
            foreach (var childId in childIds)
            {
                var child = await store.GetWorkflowInstance(childId);
                if (child.Status != WorkflowStatus.Runnable && child.Status != WorkflowStatus.Suspended)
                    continue;

                if (!await controller.TerminateWorkflow(childId))
                    logger.LogWarning("Could not terminate nested workflow {ChildId}, it is locked", childId);
            }
        }

        public static string GetChildId(ExecutionPointer pointer)
        {
            if (pointer.ExtensionAttributes != null && pointer.ExtensionAttributes.TryGetValue(RunWorkflow.ChildWorkflowIdKey, out var id))
                return id?.ToString();
            return null;
        }

        public static IEnumerable<string> GetActiveChildIds(WorkflowInstance wf)
        {
            return wf.ExecutionPointers
                .Where(p => p.EndTime == null)
                .Select(GetChildId)
                .Where(id => id != null)
                .ToList();
        }
    }
}
