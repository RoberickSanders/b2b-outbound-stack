"""
Checkpoint system for the lead pipeline.
Manages pipeline checkpoints for crash resilience with atomic writes.
"""

import os
import json
from datetime import datetime

import config


def get_checkpoint_path():
    """Return the path to the pipeline checkpoint file."""
    return os.path.join(config.OUTPUT_DIR, "pipeline_checkpoint.json")


def save_checkpoint(step, data):
    """Atomically save a pipeline checkpoint to disk."""
    checkpoint = {
        "step": step,
        "data": data,
        "timestamp": datetime.now().isoformat(),
    }
    path = get_checkpoint_path()
    temp = path + ".tmp"
    try:
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        os.replace(temp, path)
    except (IOError, OSError):
        pass


def load_checkpoint():
    """Load and return the checkpoint dict, or None if no checkpoint exists."""
    path = get_checkpoint_path()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return None
    return None


def clear_checkpoint():
    """Delete the checkpoint file if it exists."""
    path = get_checkpoint_path()
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass
