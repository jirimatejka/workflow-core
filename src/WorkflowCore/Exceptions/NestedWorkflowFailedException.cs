using System;
using WorkflowCore.Models;

namespace WorkflowCore.Exceptions
{
    public class NestedWorkflowFailedException : Exception
    {
        public string WorkflowId { get; }

        public WorkflowStatus Status { get; }

        public object Data { get; }

        public NestedWorkflowFailedException(string workflowId, WorkflowStatus status, object data)
            : base($"Nested workflow {workflowId} ended with status {status}")
        {
            WorkflowId = workflowId;
            Status = status;
            Data = data;
        }
    }
}
