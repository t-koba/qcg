"""Identical deterministic input and artifact validation, without an LLM."""
from pathlib import Path
from typing import TypedDict
import json

class State(TypedDict):
    payload: str
    output: str
    approval: bool
    approved: bool

def canonical(index: int) -> str:
    return json.dumps({"name": "qcg-comparison", "items": [1, 2, 3], "trial": index}, sort_keys=True, separators=(",", ":"))

def prepare(state: State) -> dict:
    target = Path(state["output"])
    target.mkdir(parents=True, exist_ok=True)
    with (target / "prepared.json").open("x") as file:
        file.write(state["payload"])
    return {}

def emit(state: State) -> dict:
    target = Path(state["output"])
    with (target / "result.json").open("x") as file:
        file.write(state["payload"])
    return {}

def validate(output: str, payload: str) -> None:
    assert (Path(output) / "prepared.json").read_text() == payload
    assert (Path(output) / "result.json").read_text() == payload
    assert json.loads(payload)["items"] == [1, 2, 3]
