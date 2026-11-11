# Nested workflows

The `RunWorkflow` step starts another workflow (the child) as a regular, separate workflow instance, passes it data and **waits until the child ends** – even if the child itself waits for events, activities or user actions. The child's result is then mapped back to the parent.

## Fluent API

```c#
public class ParentWorkflow : IWorkflow<ParentData>
{
    public string Id => "ParentWorkflow";
    public int Version => 1;

    public void Build(IWorkflowBuilder<ParentData> builder)
    {
        builder
            .StartWith<Prepare>()
            .RunWorkflow("ChildWorkflow", data => data.ChildInput)
                .Output(data => data.ChildResult, step => step.Result)
            .Then<Continue>();
    }
}
```

`RunWorkflow(workflowId, data, cancelCondition)` also has an overload taking an expression for the workflow id. A specific child version can be selected with `.Input(step => step.Version, data => 2)`.

## JSON / YAML

```json
{
  "Id": "run-child",
  "StepType": "WorkflowCore.Primitives.RunWorkflow, WorkflowCore",
  "NextStepId": "continue",
  "Inputs": {
    "WorkflowId": "\"ChildWorkflow\"",
    "Version": "1",
    "Data": "data.ChildInput"
  },
  "Outputs": {
    "ChildResult": "step.Result"
  }
}
```

```yaml
Id: run-child
StepType: WorkflowCore.Primitives.RunWorkflow, WorkflowCore
NextStepId: continue
Inputs:
  WorkflowId: '"ChildWorkflow"'
  Data: data.ChildInput
Outputs:
  ChildResult: step.Result
```

## Input and output

* `Data` must be an instance of the child definition's `DataType` (or `null`, in which case a new instance is created). A mismatch fails the step with an `InvalidOperationException`.
* `Result` is the complete data object of the child when it ends. The target property in the parent data must be of the child's `DataType` (or `object`).

## Child waiting for events

The child is a normal workflow instance, so you can publish events to it, suspend it, etc. The parent keeps waiting until the child ends. The child's id is stored in the parent's execution pointer (`ExtensionAttributes["ChildWorkflowId"]`) and the child's `Reference` is `wfc:parent:{parentId}:{pointerId}`.

## Failure

If the child ends as `Terminated`, the `RunWorkflow` step throws `NestedWorkflowFailedException` (with `WorkflowId`, `Status` and `Data`). The default error behavior is `Retry`, which would keep re-throwing the exception, so use `.OnError(WorkflowErrorHandling.Terminate)` (or another suitable behavior) on the step.

## Cascade termination

A running child is terminated when

* the parent's `RunWorkflow` step ends other than by completing (cancel condition, compensation, failure), or
* the parent is completed or terminated while the step is still waiting.

Grandchildren are terminated the same way.

## Known limitations

* Not exactly-once: a process crash between starting the child and persisting the parent can leave an orphaned child.
* Not transactional: a crash between persisting the child and publishing its completion leaves the parent waiting forever; the same applies to cascade termination.
* If the child is locked by another thread or node while terminating, it is not terminated (a warning is logged).
* Suspending the parent does not suspend the child, and a suspended child keeps the parent waiting.
* With the in-memory persistence provider, `Data` is passed by reference, so the parent sees changes made by the child. Database providers copy it.
* Cycles (A starts A) are not detected.
* `SyncWorkflowRunner` does not support nested workflows.
