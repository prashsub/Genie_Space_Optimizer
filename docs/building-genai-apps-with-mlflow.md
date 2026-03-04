# "Building My First GenAI App with MLflow" -- A Developer's Story

---

## Chapter 1: The Idea

> "I had a problem. My team runs dozens of Databricks Genie Spaces -- AI SQL assistants for business users. They work *okay*, but we kept getting complaints: wrong tables, broken SQL, missing columns. We needed a way to systematically evaluate and improve them.
>
> So I decided to build an app. A full-stack app that would take a Genie Space, evaluate how good it is, figure out what's wrong, fix it, and prove it got better. I knew I'd need to track a lot of moving parts. That's where MLflow came in -- not as a model training tool, but as the backbone of my entire GenAI development lifecycle."

---

## Chapter 2: "Where Do I Put My Stuff?" -- Experiments

> "The first thing I needed was a place to organize my work. I was going to be running evaluations over and over -- different Genie Spaces, different domains, different iterations. I needed a way to keep it all organized and findable.
>
> MLflow Experiments were the obvious choice. But I made a mistake early on: I just dumped everything into one experiment. After a week, I had 200 runs and no idea which was which.
>
> So I redesigned. Each optimization campaign gets its own experiment, scoped to the user and domain. And I tag every experiment with business context at creation time:"

```python
mlflow.set_experiment(f"/Users/{user_email}/genie-optimization/{domain}")

mlflow.set_experiment_tags({
    "genie.space_id": space_id,
    "genie.domain": domain,
    "genie.pipeline_version": __version__,
    "genie.catalog": catalog,
    "genie.schema": schema,
})
```

> "Now when someone asks 'show me the optimization history for the Sales genie space,' I can find it instantly. Tags are free. Use them generously.
>
> **What I learned:** An experiment isn't just a folder -- it's a searchable, tagged project. Set it up with intent."

---

## Chapter 3: "I Need Ground Truth" -- Evaluation Datasets

> "Before I could evaluate anything, I needed benchmark questions with known-good answers. 'What were total sales last quarter?' should produce a specific SQL query against a specific table.
>
> I started by keeping benchmarks in a JSON file. That lasted about two days. The problems: no versioning, no governance, no way to update a benchmark without losing the original, no way to share across runs.
>
> Then I found `mlflow.genai.datasets`. It lets you create evaluation datasets backed by Unity Catalog tables. That solved everything:"

```python
eval_dataset = mlflow.genai.datasets.create_dataset(
    name="my_catalog.my_schema.genie_benchmarks_sales"
)

records = []
for b in benchmarks:
    records.append({
        "inputs": {
            "question": b["question"],
            "question_id": b["id"],
        },
        "expectations": {
            "expected_response": b["expected_sql"],
            "expected_asset": "TABLE",  # or "MV", "TVF"
        },
    })

eval_dataset.merge_records(records)
```

> "The `merge_records` call is key -- it's an upsert. If I regenerate benchmarks, updated ones get new versions while unchanged ones are untouched. If a human corrects a benchmark later, that correction persists. The dataset is a living document, not a static snapshot.
>
> And because it lives in Unity Catalog, I get access control, lineage, and I can query it with SQL if I want to inspect it.
>
> **What I learned:** Don't manage eval data yourself. Let MLflow and UC handle versioning and governance. You'll need to update benchmarks constantly -- make that cheap."

---

## Chapter 4: "How Do I Measure Quality?" -- Custom Scorers

> "This was the hardest design decision in the whole project. What does 'good' mean for a Genie Space response? It's not one thing. It's many things: Did the SQL parse? Did it reference the right tables? Did it answer the actual question? Did it return the right results?
>
> I ended up with nine scorers. MLflow's `@scorer` decorator made each one simple to write. Here's the pattern I settled on:"

```python
from mlflow.entities import Feedback, AssessmentSource
from mlflow.genai.scorers import scorer

CODE_SOURCE = AssessmentSource(source_type="CODE", source_id="my_app/asset_check")

@scorer
def asset_routing_scorer(inputs: dict, outputs: dict, expectations: dict) -> Feedback:
    expected_type = expectations.get("expected_asset", "TABLE")
    actual_type = detect_asset_type(outputs.get("response", ""))
    correct = actual_type == expected_type

    return Feedback(
        name="asset_routing",
        value="yes" if correct else "no",
        rationale=f"Expected {expected_type}, got {actual_type}",
        source=CODE_SOURCE,
    )
```

> "Two things were important. First, I split scorers into CODE judges (deterministic checks I write in Python) and LLM judges (where I call an LLM to assess something subjective like 'is this SQL logically equivalent?'). The `AssessmentSource` field makes this distinction explicit:"

```python
# For deterministic checks
CODE_SOURCE = AssessmentSource(source_type="CODE", source_id="my_app/syntax_check")

# For LLM-based judgments
LLM_SOURCE = AssessmentSource(source_type="LLM_JUDGE", source_id="claude-opus-4-6")
```

> "Second, I return `Feedback` objects, not numbers. A number tells you the score dropped. A `Feedback` with a rationale tells you *why*. When I added structured metadata to the feedback -- failure type, blame set, suggested fix -- debugging went from 'something is wrong' to 'column X in table Y is causing schema accuracy failures, try adding a description':"

```python
return Feedback(
    name="schema_accuracy",
    value="no",
    rationale="Column 'rev' not found, closest match is 'revenue'",
    source=LLM_SOURCE,
    metadata={
        "failure_type": "column_mismatch",
        "blame_set": ["sales_table.revenue"],
        "counterfactual_fix": "Add alias 'rev' to column 'revenue'",
        "severity": "major",
        "confidence": 0.92,
    },
)
```

> "I assemble them all into a list -- some stateless, some needing runtime context:"

```python
def make_all_scorers(w, spark, catalog, schema) -> list:
    return [
        _make_syntax_validity_scorer(spark, catalog, schema),  # needs Spark to compile SQL
        _make_schema_accuracy_judge(w, catalog, schema),       # needs workspace client for LLM
        asset_routing_scorer,                                   # stateless, import directly
        result_correctness_scorer,                              # stateless
        # ... 5 more ...
    ]
```

> "**What I learned:** Don't build one giant evaluator. Build a panel of small, focused scorers. Return structured `Feedback`, not numbers. Your future self debugging a regression will thank you for every field in that metadata dict."

---

## Chapter 5: "I Need to See What's Happening" -- Tracing

> "My predict function does a lot: rate-limits, calls the Genie API, fetches results, executes ground-truth SQL, compares outputs. When something went wrong, I had no idea *where* in that chain it broke.
>
> Adding `@mlflow.trace` was a one-line change that gave me full visibility:"

```python
@mlflow.trace
def genie_predict_fn(question: str, expected_sql: str = "", **kwargs) -> dict:
    mlflow.update_current_trace(tags={
        "question_id": kwargs.get("question_id", ""),
        "space_id": space_id,
        "genie.iteration": str(iteration),
        "genie.lever": str(lever),
    })

    time.sleep(RATE_LIMIT_SECONDS)
    result = run_genie_query(w, space_id, question)
    genie_sql = result.get("sql", "")
    # ... execute GT SQL, compare results ...
    return {"response": genie_sql, "comparison": comparison}
```

> "Every prediction is now a trace I can inspect in the MLflow UI. But the real power came when I started tagging traces with business context -- question_id, iteration number, which optimization lever triggered this evaluation. Now I can filter: 'show me all traces from iteration 3, lever 4, where schema_accuracy failed.'
>
> Later, when I built the optimization strategist -- an LLM that decides which patches to apply -- I used `start_span` to trace its internal reasoning:"

```python
with mlflow.start_span(name="generate_strategy") as span:
    span.set_inputs({"failure_clusters": len(clusters)})

    with mlflow.start_span(name="triage_phase") as triage:
        triage_result = call_llm_for_triage(clusters)
        triage.set_outputs({"action_groups": len(triage_result)})

    with mlflow.start_span(name="detail_phase") as detail:
        detailed_plan = call_llm_for_details(triage_result)
        detail.set_outputs({"patches_proposed": len(detailed_plan)})
```

> "Now I can see the strategist's two-phase reasoning as a span tree. When it proposes a bad patch, I trace back through the spans to see what input it was working from.
>
> **What I learned:** `@mlflow.trace` is the single best investment you can make in a GenAI app. Add it early. Tag traces with business context, not just technical metadata. Use `start_span` for multi-step LLM reasoning chains."

---

## Chapter 6: "Running the Whole Thing" -- `mlflow.genai.evaluate()`

> "With scorers, data, and a traced predict function, I was ready to wire it all together. `mlflow.genai.evaluate()` is the function that runs everything:"

```python
with mlflow.start_run(run_name=f"eval_iter_{iteration}_{timestamp}") as run:
    mlflow.set_tags({
        "genie.space_id": space_id,
        "genie.iteration": str(iteration),
        "genie.eval_scope": eval_scope,
    })
    mlflow.log_params({
        "space_id": space_id,
        "benchmark_count": len(benchmarks),
        "num_scorers": len(scorers),
    })

    result = mlflow.genai.evaluate(
        predict_fn=predict_fn,
        data=eval_dataset,
        scorers=scorers,
    )

    # result.metrics has per-scorer aggregates
    for metric_name in result.metrics:
        if "/mean" in metric_name:
            print(f"{metric_name}: {result.metrics[metric_name]:.2%}")

    mlflow.log_metric("thresholds_passed", 1.0 if all_passed else 0.0)
```

> "One call runs every benchmark through my predict function, scores each with all nine judges, collects every trace, and aggregates metrics. The run captures everything: params, metrics, and artifacts.
>
> But production taught me something: GenAI evaluation involves live API calls, and live API calls fail. I built retry logic around the evaluate call -- with exponential backoff and a fallback to sequential execution when parallel evaluation hits transient issues:"

```python
for attempt in range(1, MAX_ATTEMPTS + 1):
    try:
        result = mlflow.genai.evaluate(**evaluate_kwargs)
        break
    except Exception as exc:
        if is_retryable(exc) and attempt < MAX_ATTEMPTS:
            os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"  # fall back to sequential
            continue
        raise
```

> "I also log rich artifacts alongside the metrics -- quarantined benchmarks that were invalid, detailed failure reports, judge prompt manifests:"

```python
mlflow.log_dict(
    {"quarantined": quarantined_benchmarks, "counts": precheck_counts},
    "evaluation_runtime/benchmark_precheck.json",
)
```

> "**What I learned:** `mlflow.genai.evaluate()` handles the happy path beautifully. For production, add retry logic and log failure artifacts. Log *more* than you think you need -- storage is cheap, re-running an evaluation is not."

---

## Chapter 7: "How Do I Version My App's State?" -- LoggedModel

> "This was the conceptual leap. In traditional ML, you version model weights. But my 'model' isn't weights -- it's a Genie Space configuration. Instructions, table metadata, column descriptions, join conditions, sample questions. That whole bundle is what determines quality.
>
> I used MLflow LoggedModel to version it:"

```python
def snapshot_config(space_id, config, iteration, uc_schema, ...) -> str:
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"model_snapshot_iter_{iteration}"):
        # Log the config as an artifact
        mlflow.log_dict(config, f"model_snapshots/iter_{iteration}/space_config.json")

        # Create a LoggedModel with searchable params
        model = mlflow.create_logged_model(
            name=f"genie_space_{space_id}",
            params={
                "space_id": space_id,
                "iteration": str(iteration),
                "uc_schema": uc_schema,
                "patch_count": str(len(patches)),
            },
            tags={"domain": domain, "traceability": "genie_space_optimizer"},
        )
        return model.model_id
```

> "Every iteration gets a snapshot. After the optimization loop, I promote the best one:"

```python
best_model_id = find_best_iteration(iterations)
mlflow.set_logged_model_alias(model_id=best_model_id, alias="champion")
```

> "And when something goes wrong in production, rollback is one function:"

```python
def rollback(w, model_id):
    model = mlflow.get_logged_model(model_id=model_id)
    config = json.loads(model.params["model_space_config"])
    patch_space_config(w, model.params["space_id"], config)
```

> "I also link evaluation scores directly to the model, so the model card in the UI shows quality metrics:"

```python
mlflow.log_metrics(
    {f"eval_{judge}": score for judge, score in scores.items()},
    model_id=model_id,
)
```

> "**What I learned:** In GenAI, your 'model' is usually a configuration, not weights. LoggedModel versions it with params, tags, and artifacts. Aliases give you promotion semantics. Linking metrics to models gives you a quality dashboard for free."

---

## Chapter 8: "Did That Patch Help or Hurt?" -- Feedback on Traces

> "My app has a 3-gate system: after applying optimization patches, it runs a slice evaluation, then a P0 evaluation, then a full evaluation. If any gate detects regressions, it rolls back.
>
> I needed a way to record *why* a gate passed or failed, attached to the actual traces. `mlflow.log_feedback()` gave me that:"

```python
mlflow.log_feedback(
    trace_id=trace_id,
    name="gate_slice",
    value=True,  # passed
    rationale="Lever 4 gate slice: pass. Schema accuracy +3.2%",
    source=AssessmentSource(source_type="CODE", source_id="my_app/gate"),
    metadata={
        "gate_type": "slice",
        "lever": 4,
        "iteration": 3,
    },
)
```

> "I also attach root-cause analysis to each trace. When a judge finds a failure, the structured metadata -- failure type, blame set, suggested fix -- gets logged as feedback:"

```python
mlflow.log_feedback(
    trace_id=trace_id,
    name="asi_schema_accuracy",
    value=False,
    rationale="Add alias 'rev' to column 'revenue' in sales_table",
    source=AssessmentSource(source_type="CODE", source_id="my_app/asi"),
    metadata={
        "failure_type": "column_mismatch",
        "blame_set": ["catalog.schema.sales_table.revenue"],
        "severity": "major",
    },
)
```

> "Now every trace tells a complete story: what question was asked, what Genie produced, what each judge thought, whether the gate passed, and what the root cause was. A reviewer can open one trace and understand everything.
>
> **What I learned:** Traces are living documents. Don't just record what happened -- annotate them with what you *concluded*. `log_feedback` turns traces from logs into investigation tools."

---

## Chapter 9: "Humans Need to Weigh In" -- Labeling Sessions

> "Automated judges are great, but they're not perfect. Sometimes they flag correct SQL as wrong. Sometimes the benchmark itself is wrong. I needed domain experts to review and correct.
>
> MLflow Labeling Sessions gave me a structured review workflow. I define labeling schemas -- essentially structured review forms:"

```python
from mlflow.genai.label_schemas import InputCategorical, InputText, create_label_schema

create_label_schema(
    name="judge_verdict_accuracy",
    type="feedback",
    title="Is the judge's verdict correct?",
    input=InputCategorical(options=[
        "Correct - judge is right",
        "Wrong - Genie answer is actually fine",
        "Wrong - both answers are wrong",
        "Ambiguous - question is unclear",
    ]),
    enable_comment=True,
    overwrite=True,
)

create_label_schema(
    name="corrected_expected_sql",
    type="expectation",
    title="Provide the correct expected SQL",
    input=InputText(),
    overwrite=True,
)
```

> "After an optimization run, I create a session populated with traces -- prioritizing failures and regressions, then backfilling with passing traces for spot-checking:"

```python
from mlflow.genai.labeling import create_labeling_session

session = create_labeling_session(
    name=f"review_{domain}_{run_id[:8]}",
    label_schemas=["judge_verdict_accuracy", "corrected_expected_sql"],
    assigned_users=["analyst@company.com"],
)
session.add_traces(traces_df)
```

> "The magic happens at the start of the *next* run. Before generating new benchmarks, I pull in any corrections from the previous session and sync them to the evaluation dataset:"

```python
# At the start of the next optimization run
session.sync(dataset_name=benchmark_table)  # push corrections to UC

feedback = ingest_human_feedback(prior_session_name)
corrections = feedback["corrections"]
# corrections now contains benchmark fixes, judge overrides, improvement suggestions
```

> "A domain expert corrected a benchmark's expected SQL in the labeling UI last week. This week's run automatically uses the corrected version as ground truth. The feedback loop is fully closed, and I didn't have to build any of the UI for it.
>
> **What I learned:** Build human review into your pipeline from day one, not as an afterthought. Labeling Sessions give you structured review forms, trace-level review, and automatic correction flow-back. Your judges will have bugs -- give humans a paved path to fix them."

---

## Chapter 10: "Making It Visible" -- Linking MLflow to Your UI

> "I built a nice React frontend for my app. But I realized users were living in two worlds -- my app UI for triggering runs, and the MLflow UI for understanding results. I needed to bridge them.
>
> First, I surface MLflow links directly in my app's UI:"

```tsx
const categoryConfig = {
  mlflow: {
    icon: <FlaskConical className="h-4 w-4" />,
    title: "MLflow",
    color: "text-emerald-700",
    bgColor: "bg-emerald-50",
  },
  // ... other categories ...
};
```

> "Second -- and this was a subtle but critical detail -- I pre-create the experiment using the user's on-behalf-of credentials, so it's accessible when they click through:"

```python
# In the backend route handler, using the user's OBO token
mlflow.set_tracking_uri("databricks")
os.environ["DATABRICKS_HOST"] = obo_host
os.environ["DATABRICKS_TOKEN"] = obo_token
mlflow.set_experiment(experiment_name)

# Grant the service principal CAN_MANAGE so the pipeline can write to it
ws.api_client.do("PUT", f"/api/2.0/permissions/experiments/{exp.experiment_id}", ...)
```

> "Without this, users would click the MLflow link and get a 403. Permissions matter.
>
> **What I learned:** Your users shouldn't have to know MLflow exists to benefit from it. Surface experiment links, run links, and trace links in your own UI. Handle permissions so the click-through just works."

---

## Epilogue: What I'd Do Differently

> "If I were starting over, here's what I'd do from day one:
>
> 1. **Start with `@mlflow.trace` on your predict function.** Before you build anything else. You need visibility from the first line of code.
>
> 2. **Register your prompts immediately.** Even your first rough draft. Version 1 of a prompt is infinitely more useful than a string buried in source code.
>
> 3. **Create your evaluation dataset early, even if it's just 5 benchmarks.** You can't improve what you can't measure.
>
> 4. **Make scorers return `Feedback` with metadata, not numbers.** You will debug regressions. Structured metadata is the difference between 'something broke' and 'here's exactly what broke and here's a suggested fix.'
>
> 5. **Use LoggedModel even if you don't have 'models.'** Your configuration, your prompt collection, your system state -- version it as a LoggedModel. You'll need rollback sooner than you think."

---

> "That's the story. MLflow isn't a model training tool that happens to support GenAI. It's a GenAI lifecycle platform -- experiments, prompts, datasets, evaluation, tracing, versioning, human review -- that happens to also support model training. Once I stopped thinking of it as the former and started treating it as the latter, everything clicked."
