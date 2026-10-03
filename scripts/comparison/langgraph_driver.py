"""Persistent graph: preparation -> human gate -> exactly-once fixture write."""
import json
import sys
from langgraph.graph import START, END, StateGraph
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command, interrupt
from workloads import State, prepare, emit

def approval(state: State) -> dict:
    approved = bool(interrupt({"approved": True})) if state["approval"] else True
    if not approved:
        raise ValueError("approval refused")
    return {"approved": approved}

def execute(db: str, thread: str, state: State | None) -> dict:
    builder = StateGraph(State)
    builder.add_node("prepare", prepare)
    builder.add_node("approval", approval)
    builder.add_node("emit", emit)
    builder.add_edge(START, "prepare")
    builder.add_edge("prepare", "approval")
    builder.add_edge("approval", "emit")
    builder.add_edge("emit", END)
    with SqliteSaver.from_conn_string(db) as saver:
        graph = builder.compile(checkpointer=saver)
        return graph.invoke(state if state is not None else Command(resume=True), {"configurable": {"thread_id": thread}})

if __name__ == "__main__":
    db, thread, mode = sys.argv[1:4]
    result = execute(db, thread, json.loads(sys.stdin.read()) if mode == "start" else None)
    print(json.dumps({"waiting": bool(result.get("__interrupt__"))}))
