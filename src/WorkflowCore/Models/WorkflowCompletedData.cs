namespace WorkflowCore.Models
{
    public class WorkflowCompletedData
    {
        public string WorkflowId { get; set; }

        public WorkflowStatus Status { get; set; }

        public object Data { get; set; }
    }
}
