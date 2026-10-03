import asyncio
import sys
from temporalio.client import Client
from temporalio.worker import Worker
from temporal_workflow import ArtifactWorkflow, prepare_activity, emit_activity

async def main():
    client = await Client.connect(sys.argv[1])
    worker = Worker(client, task_queue=sys.argv[2], workflows=[ArtifactWorkflow], activities=[prepare_activity, emit_activity])
    print("ready", flush=True)
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())
