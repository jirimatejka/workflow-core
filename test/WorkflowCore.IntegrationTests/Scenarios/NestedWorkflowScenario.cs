using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FluentAssertions;
using WorkflowCore.Exceptions;
using WorkflowCore.Interface;
using WorkflowCore.Models;
using WorkflowCore.Models.LifeCycleEvents;
using WorkflowCore.Primitives;
using WorkflowCore.Testing;
using Xunit;

namespace WorkflowCore.IntegrationTests.Scenarios
{
    public static class NestedAssets
    {
        public const string WaitEvent = "NestedChildGo";

        public class ChildData
        {
            public string Value { get; set; }
            public string Output { get; set; }
        }

        public class ParentData
        {
            public object ChildInput { get; set; }
            public ChildData ChildResult { get; set; }
            public bool Cancel { get; set; }
            public string Key { get; set; }
            public int Counter { get; set; }
            public List<string> Results { get; set; } = new List<string>();
        }

        public class Echo : StepBody
        {
            public string In { get; set; }
            public string Out { get; set; }

            public override ExecutionResult Run(IStepExecutionContext context)
            {
                Out = In + "!";
                return ExecutionResult.Next();
            }
        }

        public class EchoChild : IWorkflow<ChildData>
        {
            public string Id => "NestedEchoChild";
            public int Version => 1;

            public void Build(IWorkflowBuilder<ChildData> builder)
            {
                builder
                    .StartWith<Echo>()
                        .Input(s => s.In, d => d.Value)
                        .Output(d => d.Output, s => s.Out);
            }
        }

        public class WaitChild : IWorkflow<ChildData>
        {
            public string Id => "NestedWaitChild";
            public int Version => 1;

            public void Build(IWorkflowBuilder<ChildData> builder)
            {
                builder
                    .StartWith(ctx => ExecutionResult.Next())
                    .WaitFor(WaitEvent, d => d.Value)
                        .Output(d => d.Output, s => s.EventData);
            }
        }

        public static string ChildId(WorkflowInstance parent, int pointerIndex = 0)
        {
            return parent.ExecutionPointers
                .Where(p => p.ExtensionAttributes.ContainsKey("ChildWorkflowId"))
                .OrderBy(p => p.StartTime)
                .ElementAt(pointerIndex)
                .ExtensionAttributes["ChildWorkflowId"].ToString();
        }

        public static void WaitUntil(Func<bool> condition, int seconds = 30)
        {
            var end = DateTime.UtcNow.AddSeconds(seconds);
            while (!condition() && DateTime.UtcNow < end)
                Thread.Sleep(50);
        }
    }

    public class NestedHappyPathScenario : WorkflowTest<NestedHappyPathScenario.Parent, NestedAssets.ParentData>
    {
        public class Parent : IWorkflow<NestedAssets.ParentData>
        {
            public string Id => "NestedHappyParent";
            public int Version => 1;

            public void Build(IWorkflowBuilder<NestedAssets.ParentData> builder)
            {
                builder
                    .StartWith(ctx => ExecutionResult.Next())
                    .RunWorkflow("NestedEchoChild", d => d.ChildInput)
                        .Output(d => d.ChildResult, s => (NestedAssets.ChildData)s.Result);
            }
        }

        public NestedHappyPathScenario()
        {
            Setup();
            Host.RegisterWorkflow<NestedAssets.EchoChild, NestedAssets.ChildData>();
        }

        [Fact]
        public void should_run_child_and_map_result()
        {
            var id = StartWorkflow(new NestedAssets.ParentData { ChildInput = new NestedAssets.ChildData { Value = "hello" } });
            WaitForWorkflowToComplete(id, TimeSpan.FromSeconds(30));

            GetStatus(id).Should().Be(WorkflowStatus.Complete);
            UnhandledStepErrors.Count.Should().Be(0);
            GetData(id).ChildResult.Output.Should().Be("hello!");

            var children = PersistenceProvider.GetWorkflowInstances(null, "NestedEchoChild", null, null, 0, 100).Result.ToList();
            children.Should().HaveCount(1);
            children[0].Status.Should().Be(WorkflowStatus.Complete);
            children[0].Reference.Should().StartWith(RunWorkflow.ReferencePrefix);
        }
    }

    public class NestedWaitScenario : WorkflowTest<NestedWaitScenario.Parent, NestedAssets.ParentData>
    {
        public class Parent : IWorkflow<NestedAssets.ParentData>
        {
            public string Id => "NestedWaitParent";
            public int Version => 1;

            public void Build(IWorkflowBuilder<NestedAssets.ParentData> builder)
            {
                builder
                    .StartWith(ctx => ExecutionResult.Next())
                    .RunWorkflow("NestedWaitChild", d => d.ChildInput)
                        .OnError(WorkflowErrorHandling.Terminate)
                        .Output(d => d.ChildResult, s => (NestedAssets.ChildData)s.Result);
            }
        }

        public NestedWaitScenario()
        {
            Setup();
            Host.RegisterWorkflow<NestedAssets.WaitChild, NestedAssets.ChildData>();
        }

        private (string parentId, string childId, string key) StartAndWaitForChild()
        {
            var key = Guid.NewGuid().ToString();
            var parentId = StartWorkflow(new NestedAssets.ParentData { ChildInput = new NestedAssets.ChildData { Value = key } });
            WaitForEventSubscription(NestedAssets.WaitEvent, key, TimeSpan.FromSeconds(30));
            NestedAssets.WaitUntil(() => PersistenceProvider.GetWorkflowInstance(parentId).Result.ExecutionPointers
                .Any(p => p.Status == PointerStatus.WaitingForEvent));
            var parent = PersistenceProvider.GetWorkflowInstance(parentId).Result;
            return (parentId, NestedAssets.ChildId(parent), key);
        }

        [Fact]
        public void should_wait_for_child_waiting_on_event()
        {
            var (parentId, childId, key) = StartAndWaitForChild();

            GetStatus(parentId).Should().Be(WorkflowStatus.Runnable);
            GetStatus(childId).Should().Be(WorkflowStatus.Runnable);

            Host.PublishEvent(NestedAssets.WaitEvent, key, "go");
            WaitForWorkflowToComplete(parentId, TimeSpan.FromSeconds(30));

            GetStatus(childId).Should().Be(WorkflowStatus.Complete);
            GetStatus(parentId).Should().Be(WorkflowStatus.Complete);
            GetData(parentId).ChildResult.Output.Should().Be("go");
            UnhandledStepErrors.Count.Should().Be(0);
        }

        [Fact]
        public void should_fail_parent_when_child_terminated()
        {
            var (parentId, childId, _) = StartAndWaitForChild();

            Host.TerminateWorkflow(childId).Result.Should().BeTrue();
            WaitForWorkflowToComplete(parentId, TimeSpan.FromSeconds(30));

            GetStatus(parentId).Should().Be(WorkflowStatus.Terminated);
            UnhandledStepErrors.Select(e => e.Exception).OfType<NestedWorkflowFailedException>()
                .Should().ContainSingle(e => e.Status == WorkflowStatus.Terminated);
        }

        [Fact]
        public void should_terminate_child_when_parent_terminated()
        {
            var (parentId, childId, _) = StartAndWaitForChild();

            Host.TerminateWorkflow(parentId).Result.Should().BeTrue();
            NestedAssets.WaitUntil(() => GetStatus(childId) == WorkflowStatus.Terminated);

            GetStatus(parentId).Should().Be(WorkflowStatus.Terminated);
            GetStatus(childId).Should().Be(WorkflowStatus.Terminated);
        }
    }

    public class NestedCancelScenario : WorkflowTest<NestedCancelScenario.Parent, NestedAssets.ParentData>
    {
        public class Parent : IWorkflow<NestedAssets.ParentData>
        {
            public string Id => "NestedCancelParent";
            public int Version => 1;

            public void Build(IWorkflowBuilder<NestedAssets.ParentData> builder)
            {
                builder
                    .StartWith(ctx => ExecutionResult.Next())
                    .Parallel()
                        .Do(branch => branch
                            .StartWith(ctx => ExecutionResult.Next())
                            .RunWorkflow("NestedWaitChild", d => d.ChildInput, cancelCondition: d => d.Cancel))
                        .Do(branch => branch
                            .StartWith(ctx => ExecutionResult.Next())
                            .WaitFor("NestedCancel", d => d.Key)
                            .Then(ctx =>
                            {
                                ((NestedAssets.ParentData)ctx.Workflow.Data).Cancel = true;
                                return ExecutionResult.Next();
                            }))
                    .Join();
            }
        }

        public NestedCancelScenario()
        {
            Setup();
            Host.RegisterWorkflow<NestedAssets.WaitChild, NestedAssets.ChildData>();
        }

        [Fact]
        public void should_terminate_child_when_cancel_condition_met()
        {
            var key = Guid.NewGuid().ToString();
            var parentId = StartWorkflow(new NestedAssets.ParentData { Key = key, ChildInput = new NestedAssets.ChildData { Value = key } });
            WaitForEventSubscription(NestedAssets.WaitEvent, key, TimeSpan.FromSeconds(30));
            WaitForEventSubscription("NestedCancel", key, TimeSpan.FromSeconds(30));
            var childId = NestedAssets.ChildId(PersistenceProvider.GetWorkflowInstance(parentId).Result);

            Host.PublishEvent("NestedCancel", key, null);
            WaitForWorkflowToComplete(parentId, TimeSpan.FromSeconds(30));
            NestedAssets.WaitUntil(() => GetStatus(childId) == WorkflowStatus.Terminated);

            GetStatus(parentId).Should().Be(WorkflowStatus.Complete);
            GetStatus(childId).Should().Be(WorkflowStatus.Terminated);
            UnhandledStepErrors.Count.Should().Be(0);
        }
    }

    public class NestedLoopScenario : WorkflowTest<NestedLoopScenario.Parent, NestedAssets.ParentData>
    {
        public class Parent : IWorkflow<NestedAssets.ParentData>
        {
            public string Id => "NestedLoopParent";
            public int Version => 1;

            public void Build(IWorkflowBuilder<NestedAssets.ParentData> builder)
            {
                var prepare = builder.StartWith(ctx =>
                {
                    var d = (NestedAssets.ParentData)ctx.Workflow.Data;
                    d.ChildInput = new NestedAssets.ChildData { Value = $"{d.Key}-{d.Counter}" };
                    return ExecutionResult.Next();
                });

                var run = prepare
                    .RunWorkflow("NestedWaitChild", d => d.ChildInput)
                        .Output(d => d.ChildResult, s => (NestedAssets.ChildData)s.Result);

                run
                    .Then(ctx =>
                    {
                        var d = (NestedAssets.ParentData)ctx.Workflow.Data;
                        d.Results.Add(d.ChildResult.Output);
                        d.Counter++;
                        d.ChildInput = new NestedAssets.ChildData { Value = $"{d.Key}-{d.Counter}" };
                        return ExecutionResult.Next();
                    })
                    .Decide(d => d.Counter < 2)
                    .When(true).Then(run);
            }
        }

        public NestedLoopScenario()
        {
            Setup();
            Host.RegisterWorkflow<NestedAssets.WaitChild, NestedAssets.ChildData>();
        }

        [Fact]
        public void should_start_new_child_for_each_pass_and_not_restart_on_reexecution()
        {
            var runStepStarts = new List<StepStarted>();
            Host.OnLifeCycleEvent += evt =>
            {
                if (evt is StepStarted started)
                    lock (runStepStarts) runStepStarts.Add(started);
            };

            var key = Guid.NewGuid().ToString();
            var parentId = StartWorkflow(new NestedAssets.ParentData { Key = key });

            WaitForEventSubscription(NestedAssets.WaitEvent, $"{key}-0", TimeSpan.FromSeconds(30));
            NestedAssets.WaitUntil(() => PersistenceProvider.GetWorkflowInstance(parentId).Result.ExecutionPointers
                .Any(p => p.Status == PointerStatus.WaitingForEvent));

            Host.PublishEvent(NestedAssets.WaitEvent, $"{key}-0", "first");

            WaitForEventSubscription(NestedAssets.WaitEvent, $"{key}-1", TimeSpan.FromSeconds(30));
            NestedAssets.WaitUntil(() => PersistenceProvider.GetWorkflowInstance(parentId).Result.ExecutionPointers
                .Count(p => p.ExtensionAttributes.ContainsKey("ChildWorkflowId")) == 2
                && PersistenceProvider.GetWorkflowInstance(parentId).Result.ExecutionPointers
                    .Any(p => p.Status == PointerStatus.WaitingForEvent));

            // force a real re-execution of the waiting pointer before its event arrives
            Host.SuspendWorkflow(parentId).Result.Should().BeTrue();
            var parent = PersistenceProvider.GetWorkflowInstance(parentId).Result;
            var waiting = parent.ExecutionPointers.Single(p => p.Status == PointerStatus.WaitingForEvent);
            int startsBefore;
            lock (runStepStarts) startsBefore = runStepStarts.Count(s => s.ExecutionPointerId == waiting.Id);
            waiting.Active = true;
            PersistenceProvider.PersistWorkflow(parent).Wait();
            Host.ResumeWorkflow(parentId).Result.Should().BeTrue();

            NestedAssets.WaitUntil(() =>
            {
                lock (runStepStarts) return runStepStarts.Count(s => s.ExecutionPointerId == waiting.Id) > startsBefore;
            });
            lock (runStepStarts) runStepStarts.Count(s => s.ExecutionPointerId == waiting.Id).Should().BeGreaterThan(startsBefore);

            Host.PublishEvent(NestedAssets.WaitEvent, $"{key}-1", "second");
            WaitForWorkflowToComplete(parentId, TimeSpan.FromSeconds(30));

            GetStatus(parentId).Should().Be(WorkflowStatus.Complete);
            UnhandledStepErrors.Count.Should().Be(0);
            GetData(parentId).Results.Should().Equal("first", "second");

            parent = PersistenceProvider.GetWorkflowInstance(parentId).Result;
            var runPointers = parent.ExecutionPointers.Where(p => p.ExtensionAttributes.ContainsKey("ChildWorkflowId")).ToList();
            runPointers.Should().HaveCount(2);
            runPointers.Select(p => p.StepId).Distinct().Should().HaveCount(1);
            runPointers.Select(p => p.Id).Distinct().Should().HaveCount(2);

            var children = PersistenceProvider.GetWorkflowInstances(null, "NestedWaitChild", null, null, 0, 100).Result.ToList();
            children.Should().HaveCount(2);
            children.Should().OnlyContain(c => c.Status == WorkflowStatus.Complete);
            foreach (var p in runPointers)
            {
                var child = children.Single(c => c.Id == p.ExtensionAttributes["ChildWorkflowId"].ToString());
                child.Reference.Should().Be($"{RunWorkflow.ReferencePrefix}{parentId}:{p.Id}");
            }
        }
    }

    public class NestedJsonScenario : JsonWorkflowTest
    {
        private const string Child = @"{
  ""Id"": ""JsonNestedChild"",
  ""Version"": 1,
  ""DataType"": ""WorkflowCore.IntegrationTests.Scenarios.NestedAssets+ChildData, WorkflowCore.IntegrationTests"",
  ""Steps"": [
    {
      ""Id"": ""echo"",
      ""StepType"": ""WorkflowCore.IntegrationTests.Scenarios.NestedAssets+Echo, WorkflowCore.IntegrationTests"",
      ""Inputs"": { ""In"": ""data.Value"" },
      ""Outputs"": { ""Output"": ""step.Out"" }
    }
  ]
}";

        private const string Parent = @"{
  ""Id"": ""JsonNestedParent"",
  ""Version"": 1,
  ""DataType"": ""WorkflowCore.IntegrationTests.Scenarios.NestedAssets+ParentData, WorkflowCore.IntegrationTests"",
  ""Steps"": [
    {
      ""Id"": ""run-child"",
      ""StepType"": ""WorkflowCore.Primitives.RunWorkflow, WorkflowCore"",
      ""Inputs"": {
        ""WorkflowId"": ""\""JsonNestedChild\"""",
        ""Version"": ""1"",
        ""Data"": ""data.ChildInput""
      },
      ""Outputs"": { ""ChildResult"": ""step.Result"" }
    }
  ]
}";

        public NestedJsonScenario()
        {
            Setup();
        }

        [Fact]
        public void should_run_nested_json_workflow()
        {
            DefinitionLoader.LoadDefinition(Child, WorkflowCore.Services.DefinitionStorage.Deserializers.Json);
            var id = StartWorkflow(Parent, new NestedAssets.ParentData { ChildInput = new NestedAssets.ChildData { Value = "json" } });
            WaitForWorkflowToComplete(id, TimeSpan.FromSeconds(30));

            GetStatus(id).Should().Be(WorkflowStatus.Complete);
            UnhandledStepErrors.Count.Should().Be(0);
            GetData<NestedAssets.ParentData>(id).ChildResult.Output.Should().Be("json!");
        }
    }
}
