# Nested workflow (v1, varianta A)

KISS / YAGNI. Žádné `ParentId`, žádná migrace persistence, žádný sample projekt, žádný depth cap, žádná podpora v `SyncWorkflowRunner`. Jen to, co je v tomto zadání.

## Cíl

Parent workflow spustí child workflow jako step, předá mu data, **počká, až child skončí** (i když child mezitím čeká na `WaitFor` / activity / user task), a namapuje výsledek zpět. Child je běžná samostatná `WorkflowInstance`, na kterou lze normálně posílat eventy (`PublishEvent`), suspendovat ji atd.

Podporováno ve Fluent API i v JSON/YAML DSL.

## Rozhodnutí (zamčená)

| Téma | Volba |
|---|---|
| Mechanismus čekání | parent step = běžný `WaitForEvent` na persistovaný event, který vyšle child při dokončení |
| Neúspěch childu (`Terminated`) | parent step hodí `NestedWorkflowFailedException` (analogie `ActivityFailedException`) |
| Vazba parent → child | `ExecutionPointer.ExtensionAttributes["ChildWorkflowId"]` (persistují ho všechny providery) |
| Vazba child → parent | `WorkflowInstance.Reference` s pevným prefixem (viz níže), bez změny schématu |
| Cascade | když pointer `RunWorkflow` skončí jinak než `Complete` (cancel, compensate, …) nebo když parent skončí (`Terminated`/`Complete`) a child ještě běží → child se terminuje |
| API | stejný vzor jako `Activity` (`Input` / `Output`) |

---

## Chování

### Konstanty a payload

Do `src/WorkflowCore/Models/Event.cs` vedle `EventTypeActivity`:

```csharp
public const string EventTypeWorkflowCompleted = "WorkflowCore.WorkflowCompleted";
```

Nový soubor `src/WorkflowCore/Models/WorkflowCompletedData.cs` (jednoduché POCO s public settery — musí jít serializovat přes JSON `TypeNameHandling` v EF providerech):

```csharp
public class WorkflowCompletedData
{
    public string WorkflowId { get; set; }
    public WorkflowStatus Status { get; set; }   // Complete | Terminated
    public object Data { get; set; }             // child.Data v okamžiku dokončení
}
```

Reference childu:

```csharp
// v RunWorkflow (public const, ať je to dohledatelné)
public const string ReferencePrefix = "wfc:parent:";
reference = $"{ReferencePrefix}{context.Workflow.Id}:{context.ExecutionPointer.Id}";
```

Prefix slouží jako příznak „tato instance je child“ — completion event se publikuje **jen** pro takové instance (viz níže proč).

### Step `RunWorkflow`

`RunWorkflow : StepBodyAsync`, ctor `RunWorkflow(IWorkflowController controller, IWorkflowRegistry registry)`.  
**Ne** `IWorkflowHost` (tranzitivně táhne background tasky).

**1. průchod** (`EventPublished == false`):

1. Pokud `pointer.ExtensionAttributes` už obsahuje `"ChildWorkflowId"` → **nespouštět znovu**, jen znovu vrátit `WaitForEvent` (bod 5) s uloženým id. Hodnotu číst přes `?.ToString()` — po deserializaci z DB to nemusí být `string`.
2. Ověřit vstup: `WorkflowId` nesmí být prázdné; `registry.GetDefinition(WorkflowId, Version)` nesmí být `null` (jinak `WorkflowNotRegisteredException`); pokud `Data != null` a `def.DataType != null` a `!def.DataType.IsInstanceOfType(Data)` → `InvalidOperationException` se srozumitelnou zprávou (jinak by child padal až v prvním kroku na `InvalidCastException`).
3. `childId = await controller.StartWorkflow(WorkflowId, Version, Data, reference)`.  
   `Data == null` je OK — engine vytvoří novou instanci `def.DataType`.
4. `pointer.ExtensionAttributes["ChildWorkflowId"] = childId`.
5. `return ExecutionResult.WaitForEvent(Event.EventTypeWorkflowCompleted, childId, DateTime.MinValue);`

`EventAsOf` **musí být `DateTime.MinValue`**. Child je zařazen do fronty hned v `StartWorkflow`, tedy dřív, než se persistuje subscription parenta. Pokud child doběhne dřív, jeho event zůstane v tomto forku nezpracovaný (`EventConsumer` bez subscriberů event nechá `IsProcessed == false`) a `WorkflowConsumer.TryProcessSubscription` ho po vytvoření subscription znovu zařadí. To funguje jen pokud `SubscribeAsOf <= EventTime`.

**2. průchod** (`EventPublished == true`):

- `context.ExecutionPointer.EventData` je `WorkflowCompletedData`.
- `Status == Complete` → `Result = data.Data`, `return ExecutionResult.Next()`.
- jinak → `throw new NestedWorkflowFailedException(data.WorkflowId, data.Status, data.Data)`.
- Ve 2. průchodu **nikdy** nespouštět child (důležité pro `Retry` error behavior — viz testy).

`Input`/`Output` mapování neřešit — engine ho aplikuje jako u každého stepu (`MemberMapParameter` přetypuje `object` na cílový typ property).

### Publikace completion eventu

Helper (jeden, `internal static`), např. `src/WorkflowCore/Services/NestedWorkflowHelper.cs`:

```csharp
internal static Task PublishCompletion(IPersistenceProvider store, IQueueProvider queue, IDateTimeProvider clock, WorkflowInstance wf)
```

- No-op, pokud `wf.Reference` nezačíná `RunWorkflow.ReferencePrefix`.
- Jinak `store.CreateEvent(new Event { EventName = EventTypeWorkflowCompleted, EventKey = wf.Id, EventTime = clock.UtcNow, EventData = new WorkflowCompletedData {...}, IsProcessed = false })` + `queue.QueueWork(eventId, QueueType.Event)`.

**Proč filtr na prefix:** v tomto forku zůstává event bez subscriberu nezpracovaný a `RunnablePoller` ho opakovaně vybírá přes `GetRunnableEvents`. Publikovat event pro *každé* dokončené workflow by trvale zatěžovalo poller a plnilo tabulku eventů.

Volání (jen tato dvě místa, **ne** `WorkflowExecutor` — ten nemá `IPersistenceProvider` ani `IQueueProvider` a event musí vzniknout až po persistenci childu):

1. `src/WorkflowCore/Services/BackgroundTasks/WorkflowConsumer.cs` — před `_executor.Execute` si uložit `previousStatus = workflow.Status`. Ve `finally` po `ReleaseLock` (blok `if ((workflow != null) && (result != null))`): pokud `previousStatus == Runnable` a nový status je `Complete` nebo `Terminated` → `PublishCompletion`.  
   Pokrývá všechny cesty: běžné dokončení v `DetermineNextExecutionTime`, `ExecutionPipelineDirective.EndWorkflow` (ten nastaví `Complete` bez lifecycle eventu!) i `TerminateHandler`.
2. `src/WorkflowCore/Services/WorkflowController.cs` `TerminateWorkflow` — po `PersistWorkflow`, **jen** pokud původní status byl `Runnable` nebo `Suspended` (současný `TerminateWorkflow` status nekontroluje a „terminuje“ i dokončené instance; to chování neměnit, jen v tom případě nepublikovat).

### Cascade terminate

Pravidlo: **child se terminuje, když jeho `RunWorkflow` pointer skončí jinak než `Complete`, nebo když parent přejde do `Complete`/`Terminated` a pointer stále čeká.** Rekurze (vnuk) funguje sama, protože terminace childu jde přes `WorkflowController.TerminateWorkflow`.

Helper (ve stejné třídě):

```csharp
internal static async Task TerminateChildren(IWorkflowController controller, IPersistenceProvider store, ILogger logger, IEnumerable<string> childIds)
```

- pro každé id načíst instanci; pokud není `Runnable`/`Suspended`, přeskočit;
- jinak `controller.TerminateWorkflow(childId)`; při `false` (lock drží někdo jiný) jen `LogWarning` — žádný retry framework.

Volání:

1. `WorkflowConsumer` — před `Execute` si zapamatovat množinu pointerů `{ ptr.Id → childId }`, které mají `ChildWorkflowId` a `EndTime == null`. Po persistenci (stejné místo jako publikace eventu) terminovat child pro každý takový pointer, který:
   - má nyní `EndTime != null` a `Status != PointerStatus.Complete` (pokrývá `CancelCondition` včetně kaskády na descendenty v `CancellationProcessor`, compensate, fail), **nebo**
   - parent je nyní `Complete`/`Terminated` a pointer pořád nemá `EndTime` (pokrývá `TerminateHandler` a `EndWorkflow` v paralelní větvi).
2. `WorkflowController.TerminateWorkflow(parent)` — po persistenci parenta terminovat child všech pointerů s `ChildWorkflowId` a `EndTime == null`.

Tím **neměníme** `ICancellationProcessor`, `IWorkflowErrorHandler`, `CompensateHandler` ani `TerminateHandler` a nevoláme I/O na child uvnitř exekuce parenta (pod jeho lockem, před jeho persistencí).

`WorkflowConsumer` k tomu potřebuje `IWorkflowController` v ctoru (singleton, nezávisí na executoru → bez cyklu v DI).

Child terminovaný kaskádou pošle completion event s `Terminated`; `EventConsumer` ho doručí do parenta, ale pointer už má `EndTime` (resp. parent není `Runnable`), takže se nic nestane. To je očekávané.

### DI

`ConstructBody` bere `GetService(BodyType)`, jinak bezparametrický ctor (který `RunWorkflow` nemá). V `ServiceCollectionExtensions.AddWorkflow` vedle `services.AddTransient<Foreach>()`:

```csharp
services.AddTransient<RunWorkflow>();
```

Platí i pro JSON/YAML — `DefinitionLoader` jen resolvuje typ, body se konstruuje stejně.

---

## API (nic víc)

### Step

Soubor: `src/WorkflowCore/Primitives/RunWorkflow.cs`. Smazat stub `src/WorkflowCore/Primitives/SubWorkflowStepBody.cs` (nikde se nepoužívá).

```csharp
public class RunWorkflow : StepBodyAsync
{
    public const string ReferencePrefix = "wfc:parent:";

    public string WorkflowId { get; set; }
    public int? Version { get; set; }
    public object Data { get; set; }
    public object Result { get; set; }

    public RunWorkflow(IWorkflowController controller, IWorkflowRegistry registry) { ... }
    public override Task<ExecutionResult> RunAsync(IStepExecutionContext context) { ... }
}
```

### Výjimka

`src/WorkflowCore/Exceptions/NestedWorkflowFailedException.cs` — analogie `ActivityFailedException`, s vlastnostmi `string WorkflowId`, `WorkflowStatus Status`, `object Data` a zprávou typu `$"Nested workflow {id} ended with status {status}"`.

### Fluent

Stejný styl jako `Activity` (`IWorkflowModifier.cs`), jen id + data + volitelný cancel. **Bez** `effectiveDate`. `Version` jen přes `.Input(step => step.Version, ...)`.

```csharp
IStepBuilder<TData, RunWorkflow> RunWorkflow(
    string workflowId,
    Expression<Func<TData, object>> data = null,
    Expression<Func<TData, bool>> cancelCondition = null);

IStepBuilder<TData, RunWorkflow> RunWorkflow(
    Expression<Func<TData, IStepExecutionContext, string>> workflowId,
    Expression<Func<TData, object>> data = null,
    Expression<Func<TData, bool>> cancelCondition = null);
```

Soubory: `Interface/IWorkflowModifier.cs` (+ XML doc), `Services/FluentBuilders/StepBuilder.cs` (implementace dle `Activity`), `Services/FluentBuilders/WorkflowBuilder.cs` (delegace na `Start().RunWorkflow(...)` jako u `Activity`). Jiné implementace `IWorkflowModifier` v repu nejsou.

Použití:

```csharp
builder
    .StartWith<Prepare>()
    .RunWorkflow("ChildWorkflow", data => data.ChildInput)
        .Output(data => data.ChildResult, step => step.Result)
    .Then<Continue>();
```

### JSON / YAML

Žádná změna `DefinitionLoader`u ani DSL modelů. Stačí `StepType` + `Inputs`/`Outputs` (+ volitelně existující `CancelCondition`). Hodnoty `Inputs` jsou výrazy, proto je řetězcový literál v uvozovkách.

JSON:

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

YAML:

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

Pravidla typů (zdokumentovat):

- `data.ChildInput` musí být instance `DataType` child definice (nebo `null`).
- `ChildResult` v parent datech musí být typu `DataType` childu (nebo `object`); `Result` je celý `Data` childu.

---

## Známá omezení (zdokumentovat, neřešit)

- **Ne exactly-once start:** pád procesu mezi `StartWorkflow` a persistencí parenta vytvoří osiřelý child a po retry druhý. `ChildWorkflowId` chrání jen proti opakovanému spuštění po úspěšné persistenci.
- **Ne transakční completion:** pád mezi persistencí childu a `CreateEvent` → parent čeká navždy. Stejně tak cascade terminate po persistenci parenta.
- **Cascade při locku:** pokud child zrovna běží na jiném vlákně/uzlu, `TerminateWorkflow` vrátí `false` a child doběhne (jen warning).
- **Suspend:** suspend parenta childa nesuspenduje; suspendovaný child (např. `ErrorBehavior = Suspend`) drží parenta v čekání.
- **In-memory provider sdílí referenci:** `Data` se do childu předává referencí; s `MemoryPersistenceProvider` změny v childu vidí i parent. S DB providery je child data kopie. Testy ověřují výsledek jen přes `Output`.
- **Cykly** (A spouští A) se nedetekují.
- `SyncWorkflowRunner` nested workflow nepodporuje.

---

## Mimo rozsah (nedělat)

- `ParentId` / migrace persistencí / `GetByReference`
- depth cap, detekce cyklů
- podpora `SyncWorkflowRunner`
- nový sample projekt
- outcome větve Complete vs Terminated
- suspend/resume childu při suspend/resume parenta
- změny search indexu
- public `IChildWorkflowTerminator` a podobné abstrakce
- dědění z `WaitFor`
- změny `ICancellationProcessor`, `IWorkflowErrorHandler` a error handlerů

---

## Testy

### Integrační — `test/WorkflowCore.IntegrationTests/Scenarios`

Vzor `ActivityScenario` / `EventScenario` / `StoredJsonScenario`. `WorkflowTest<,>` registruje jen jedno workflow → child registrovat v ctoru po `Setup()` přes `Host.RegisterWorkflow<ChildWorkflow, ChildData>()`. Child id získat z parent pointeru (`ExtensionAttributes["ChildWorkflowId"]`) přes `PersistenceProvider.GetWorkflowInstance(parentId)`.

1. **Happy path** — parent předá objekt, child doběhne, parent `Output` dostane child data, oba `Complete`, `UnhandledStepErrors` prázdné. Navíc ověřit, že existuje právě jedna instance child definice (`PersistenceProvider.GetWorkflowInstances(null, "ChildWorkflow", null, null, 0, 100)`) a její `Reference` začíná `RunWorkflow.ReferencePrefix`.
2. **Child `WaitFor`** — child čeká na event; `WaitForEventSubscription(...)` pro child; parent je `Runnable` a jeho pointer je `WaitingForEvent`; po `Host.PublishEvent(...)` doběhne child i parent (`Complete`).
3. **Child terminate** — child čeká na `WaitFor`, test zavolá `Host.TerminateWorkflow(childId)`. Parent step má `.OnError(WorkflowErrorHandling.Terminate)` (default `Retry` by výjimku opakoval donekonečna). Ověřit: parent `Terminated`, `UnhandledStepErrors` obsahuje `NestedWorkflowFailedException` se `Status == Terminated`.
4. **Parent terminate** — child čeká na `WaitFor`, `Host.TerminateWorkflow(parentId)` → child skončí `Terminated`.
5. **Cancel condition** — `RunWorkflow(..., cancelCondition: data => data.Cancel)` v paralelní větvi s krokem, který `Cancel = true` nastaví; child (čekající na `WaitFor`) skončí `Terminated`, parent `Complete`.
6. **JSON** — parent i child jako JSON definice (`JsonWorkflowTest`, vzor `StoredJsonScenario`), happy path s `Inputs`/`Outputs` dle ukázky výše.
7. **Návrat parenta na `RunWorkflow`** — parent se po dokončení prvního childu podmíněně vrátí na stejný step `RunWorkflow` a po druhém průchodu skončí. Ověřit:
   - vznikly dva různé parent execution pointery pro stejný step a dvě různé child instance; každá child instance má v `Reference` ID parenta a odpovídajícího pointeru a každý pointer má v `ChildWorkflowId` ID svého childu;
   - parent při každém průchodu dostane výsledek odpovídajícího childu; první child zůstane `Complete` a jeho již provedený efekt se návratem nekompenzuje;
   - opětovné vykonání téhož čekajícího pointeru před doručením completion eventu nespustí další child. Test musí ověřit, že ke skutečnému opětovnému vykonání došlo, nejen pointer znovu zařadit do fronty.

### Unit — `test/WorkflowCore.UnitTests`

8. **Idempotentní start** — `RunWorkflow` s fake `IWorkflowController`: pointer už má `ChildWorkflowId` → `StartWorkflow` se nevolá a vrátí se `WaitForEvent` s uloženým id. Ve 2. průchodu (`EventPublished == true`) se `StartWorkflow` taky nevolá.
9. **Validace typu dat** — `Data` jiného typu než `DataType` childu → `InvalidOperationException`, `StartWorkflow` se nevolá.

Testy pro jednotlivé persistence providery nepsat; default in-memory host stačí. Stávající testy (`WorkflowExecutorFixture`, `SyncWorkflowRunnerTests`, …) musí dál procházet beze změny.

---

## Dokumentace

Jeden soubor `docs/nested-workflows.md` (Fluent + JSON + YAML ukázka, Input/Output a pravidla typů, WaitFor v childu, terminate → výjimka + doporučení `OnError`, cascade, známá omezení) a položka `- Nested workflows: nested-workflows.md` v `mkdocs.yml` (za „Activity workers“). Nic dalšího.

---

## Pořadí práce

1. `Event.EventTypeWorkflowCompleted`, `WorkflowCompletedData`, `NestedWorkflowFailedException`.
2. `RunWorkflow` + DI, smazat `SubWorkflowStepBody`; unit testy 8–9.
3. `NestedWorkflowHelper` + napojení do `WorkflowConsumer` a `WorkflowController.TerminateWorkflow` (publikace + cascade).
4. Fluent API.
5. Integrační testy 1–7.
6. Docs.

Hotovo, když testy 1–9 i stávající testy v `WorkflowCore.UnitTests` a `WorkflowCore.IntegrationTests` procházejí a v PR není nic mimo tento seznam.
