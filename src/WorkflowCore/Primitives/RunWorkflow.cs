using System;
using System.Threading.Tasks;
using WorkflowCore.Exceptions;
using WorkflowCore.Interface;
using WorkflowCore.Models;

namespace WorkflowCore.Primitives
{
    public class RunWorkflow : StepBodyAsync
    {
        public const string ReferencePrefix = "wfc:parent:";
        internal const string ChildWorkflowIdKey = "ChildWorkflowId";

        private readonly IWorkflowController _controller;
        private readonly IWorkflowRegistry _registry;

        public string WorkflowId { get; set; }

        public int? Version { get; set; }

        public object Data { get; set; }

        public object Result { get; set; }

        public RunWorkflow(IWorkflowController controller, IWorkflowRegistry registry)
        {
            _controller = controller;
            _registry = registry;
        }

        public override async Task<ExecutionResult> RunAsync(IStepExecutionContext context)
        {
            var pointer = context.ExecutionPointer;

            if (pointer.EventPublished)
            {
                var completed = pointer.EventData as WorkflowCompletedData;
                if (completed == null)
                    throw new InvalidOperationException("Nested workflow completion event has no data");

                if (completed.Status != WorkflowStatus.Complete)
                    throw new NestedWorkflowFailedException(completed.WorkflowId, completed.Status, completed.Data);

                Result = completed.Data;
                return ExecutionResult.Next();
            }

            if (pointer.ExtensionAttributes != null
                && pointer.ExtensionAttributes.TryGetValue(ChildWorkflowIdKey, out var existing)
                && existing != null)
            {
                return Wait(existing.ToString());
            }

            if (string.IsNullOrEmpty(WorkflowId))
                throw new ArgumentException("WorkflowId is required", nameof(WorkflowId));

            var def = _registry.GetDefinition(WorkflowId, Version);
            if (def == null)
                throw new WorkflowNotRegisteredException(WorkflowId, Version);

            if (Data != null && def.DataType != null && !def.DataType.IsInstanceOfType(Data))
                throw new InvalidOperationException($"Data of type {Data.GetType().FullName} is not compatible with nested workflow {WorkflowId} data type {def.DataType.FullName}");

            var reference = $"{ReferencePrefix}{context.Workflow.Id}:{pointer.Id}";
            var childId = await _controller.StartWorkflow(WorkflowId, Version, Data, reference);
            pointer.ExtensionAttributes[ChildWorkflowIdKey] = childId;

            return Wait(childId);
        }

        // MinValue: the child may finish before this pointer's subscription is persisted
        private static ExecutionResult Wait(string childId)
        {
            return ExecutionResult.WaitForEvent(Event.EventTypeWorkflowCompleted, childId, DateTime.MinValue);
        }
    }
}
