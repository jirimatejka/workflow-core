using System;
using System.Threading.Tasks;
using FakeItEasy;
using FluentAssertions;
using WorkflowCore.Exceptions;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Primitives;
using Xunit;

namespace WorkflowCore.UnitTests.Primitives
{
    public class RunWorkflowTests
    {
        private readonly IWorkflowController _controller = A.Fake<IWorkflowController>();
        private readonly IWorkflowRegistry _registry = A.Fake<IWorkflowRegistry>();

        private StepExecutionContext Context(ExecutionPointer pointer) => new StepExecutionContext
        {
            ExecutionPointer = pointer,
            Workflow = new WorkflowInstance { Id = "parent" }
        };

        [Fact]
        public async Task should_not_start_child_again_when_pointer_already_has_child()
        {
            var pointer = new ExecutionPointer { Id = "p1" };
            pointer.ExtensionAttributes["ChildWorkflowId"] = "child-1";
            var subject = new RunWorkflow(_controller, _registry) { WorkflowId = "Child" };

            var result = await subject.RunAsync(Context(pointer));

            result.EventName.Should().Be(Event.EventTypeWorkflowCompleted);
            result.EventKey.Should().Be("child-1");
            result.EventAsOf.Should().Be(DateTime.MinValue);
            A.CallTo(() => _controller.StartWorkflow(A<string>._, A<int?>._, A<object>._, A<string>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task should_not_start_child_when_event_published()
        {
            var pointer = new ExecutionPointer
            {
                Id = "p1",
                EventPublished = true,
                EventData = new WorkflowCompletedData { WorkflowId = "child-1", Status = WorkflowStatus.Complete, Data = "done" }
            };
            pointer.ExtensionAttributes["ChildWorkflowId"] = "child-1";
            var subject = new RunWorkflow(_controller, _registry) { WorkflowId = "Child" };

            var result = await subject.RunAsync(Context(pointer));

            result.Proceed.Should().BeTrue();
            subject.Result.Should().Be("done");
            A.CallTo(() => _controller.StartWorkflow(A<string>._, A<int?>._, A<object>._, A<string>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task should_throw_when_child_terminated()
        {
            var pointer = new ExecutionPointer
            {
                EventPublished = true,
                EventData = new WorkflowCompletedData { WorkflowId = "child-1", Status = WorkflowStatus.Terminated }
            };
            var subject = new RunWorkflow(_controller, _registry) { WorkflowId = "Child" };

            var ex = await Assert.ThrowsAsync<NestedWorkflowFailedException>(() => subject.RunAsync(Context(pointer)));

            ex.Status.Should().Be(WorkflowStatus.Terminated);
        }

        [Fact]
        public async Task should_reject_data_of_wrong_type()
        {
            A.CallTo(() => _registry.GetDefinition("Child", A<int?>._)).Returns(new WorkflowDefinition { Id = "Child", DataType = typeof(string) });
            var subject = new RunWorkflow(_controller, _registry) { WorkflowId = "Child", Data = 42 };

            await Assert.ThrowsAsync<InvalidOperationException>(() => subject.RunAsync(Context(new ExecutionPointer { Id = "p1" })));
            A.CallTo(() => _controller.StartWorkflow(A<string>._, A<int?>._, A<object>._, A<string>._)).MustNotHaveHappened();
        }

        [Fact]
        public async Task should_start_child_and_store_its_id()
        {
            A.CallTo(() => _registry.GetDefinition("Child", A<int?>._)).Returns(new WorkflowDefinition { Id = "Child", DataType = typeof(string) });
            A.CallTo(() => _controller.StartWorkflow("Child", A<int?>._, A<object>._, A<string>._)).Returns("child-1");
            var pointer = new ExecutionPointer { Id = "p1" };
            var subject = new RunWorkflow(_controller, _registry) { WorkflowId = "Child", Data = "in" };

            var result = await subject.RunAsync(Context(pointer));

            pointer.ExtensionAttributes["ChildWorkflowId"].Should().Be("child-1");
            result.EventKey.Should().Be("child-1");
            A.CallTo(() => _controller.StartWorkflow("Child", A<int?>._, "in", $"{RunWorkflow.ReferencePrefix}parent:p1")).MustHaveHappenedOnceExactly();
        }
    }
}
