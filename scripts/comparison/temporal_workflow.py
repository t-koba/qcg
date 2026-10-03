"""Temporal workflow uses activities for all filesystem effects."""
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
with workflow.unsafe.imports_passed_through():
    from workloads import State, prepare, emit

@activity.defn
async def prepare_activity(state: State) -> dict:
    return prepare(state)

@activity.defn
async def emit_activity(state: State) -> dict:
    return emit(state)

@workflow.defn
class ArtifactWorkflow:
    def __init__(self):
        self.approved = False
        self.phase = "starting"

    @workflow.signal
    def approve(self):
        self.approved = True

    @workflow.query
    def current_phase(self) -> str:
        return self.phase

    @workflow.run
    async def run(self, state: State) -> str:
        options = {"start_to_close_timeout": timedelta(seconds=20), "retry_policy": RetryPolicy(maximum_attempts=1)}
        await workflow.execute_activity(prepare_activity, state, **options)
        self.phase = "waiting" if state["approval"] else "writing"
        if state["approval"]:
            await workflow.wait_condition(lambda: self.approved)
        self.phase = "writing"
        await workflow.execute_activity(emit_activity, state, **options)
        self.phase = "finished"
        return state["payload"]
