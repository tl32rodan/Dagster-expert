"""Minimal demo of run states + DefaultRunLauncher + QueuedRunCoordinator.

The dagster.yaml config that goes with this:

    run_launcher:
      module: dagster._core.launcher
      class: DefaultRunLauncher

    run_coordinator:
      module: dagster._core.run_coordinator
      class: QueuedRunCoordinator
      config:
        max_concurrent_runs: 8

    run_monitoring: { enabled: true, start_timeout_seconds: 180, poll_interval_seconds: 60 }
    telemetry: { enabled: false }

Run: dagster definitions validate -m examples.05_run_lifecycle
"""
import dagster as dg


@dg.op
def step_a(context: dg.OpExecutionContext) -> int:
    context.log.info("running step_a")
    return 42


@dg.op
def step_b(context: dg.OpExecutionContext, x: int) -> int:
    context.log.info(f"running step_b with x={x}")
    return x + 1


@dg.job(
    tags={"dagster/concurrency_key": "demo_pool"},
)
def two_step_job():
    step_b(step_a())


defs = dg.Definitions(jobs=[two_step_job])
