import os
import pandas as pd
import json
import time
import uuid
import glob
from app.config import Config


def create_task_id():
    """Generates a unique ID for the task."""
    return str(uuid.uuid4())


def save_dataset(task_id, data, metadata, failed_ids):
    """
    Saves the extracted data to CSV and metadata to JSON locally.
    """
    df = pd.DataFrame(data)

    metadata["failed_ids"] = failed_ids
    metadata["saved_at"] = time.time()
    metadata["task_id"] = task_id
    if "status" not in metadata:
        metadata["status"] = "finished"

    if not os.path.exists(Config.TEMP_DATA_DIR):
        os.makedirs(Config.TEMP_DATA_DIR)

    csv_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.csv")
    df.to_csv(csv_path, index=False)

    json_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    return task_id


def load_dataset(task_id):
    """
    Loads CSV data and JSON metadata from local storage.
    Returns: (DataFrame, MetadataDict) or (None, None)
    """
    metadata = {}

    try:
        csv_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.csv")
        json_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.json")

        if not os.path.exists(csv_path) or not os.path.exists(json_path):
            return None, None

        with open(json_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)

        df = pd.read_csv(csv_path)

        return df, metadata

    except pd.errors.EmptyDataError:
        return pd.DataFrame(), metadata
    except Exception as e:
        print(f"Error loading cache for {task_id}: {e}")
        return None, None


def get_dataset_metadata(task_id):
    """Retrieves ONLY the metadata JSON for a task."""
    try:
        json_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.json")
        if not os.path.exists(json_path):
            return None

        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading metadata for {task_id}: {e}")
        return None


def get_history():
    """
    Scans the local storage for all completed tasks (JSON files).
    Returns a list of metadata dictionaries sorted by date (newest first).
    """
    history_items = []

    try:
        if not os.path.exists(Config.TEMP_DATA_DIR):
            return []

        json_files = glob.glob(os.path.join(Config.TEMP_DATA_DIR, "*.json"))

        for j_file in json_files:
            with open(j_file, 'r', encoding='utf-8') as f:
                meta = json.load(f)
                ts = meta.get("saved_at", 0)
                meta["date_str"] = time.strftime('%Y-%m-%d %H:%M', time.localtime(ts))
                if "status" not in meta:
                    meta["status"] = "finished"
                history_items.append(meta)

    except Exception as e:
        print(f"Error fetching history: {e}")

    history_items.sort(key=lambda x: x.get("saved_at", 0), reverse=True)
    return history_items


def get_dataset_download_info(task_id):
    """
    Returns a dictionary indicating how to download the file from the local filesystem.
    """
    path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.csv")
    if os.path.exists(path):
        return {"type": "file", "path": path}
    return None
