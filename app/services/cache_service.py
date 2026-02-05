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
    Saves the extracted data to CSV and metadata to JSON.
    """
    if not os.path.exists(Config.TEMP_DATA_DIR):
        os.makedirs(Config.TEMP_DATA_DIR)

    # 1. Save Data (CSV)
    df = pd.DataFrame(data)
    csv_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.csv")
    df.to_csv(csv_path, index=False)

    # 2. Save Metadata (JSON)
    # Ensure failed_ids is included
    metadata["failed_ids"] = failed_ids
    metadata["saved_at"] = time.time()
    metadata["task_id"] = task_id

    # Ensure status is saved (default to finished if missing)
    if "status" not in metadata:
        metadata["status"] = "finished"

    json_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)

    return task_id


def load_dataset(task_id):
    """
    Loads CSV data and JSON metadata.
    Returns: (DataFrame, MetadataDict) or (None, None)
    """
    csv_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.csv")
    json_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.json")

    if not os.path.exists(csv_path) or not os.path.exists(json_path):
        return None, None

    try:
        df = pd.read_csv(csv_path)
        with open(json_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        return df, metadata
    except pd.errors.EmptyDataError:
        # Handle case where file exists but has no data
        with open(json_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        return pd.DataFrame(), metadata
    except Exception as e:
        print(f"Error loading cache for {task_id}: {e}")
        return None, None


def get_dataset_metadata(task_id):
    """Retrieves ONLY the metadata JSON for a task."""
    json_path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.json")

    if not os.path.exists(json_path):
        return None

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading metadata for {task_id}: {e}")
        return None


def get_history():
    """
    Scans the temp_data directory for all completed tasks (JSON files).
    Returns a list of metadata dictionaries sorted by date (newest first).
    """
    if not os.path.exists(Config.TEMP_DATA_DIR):
        return []

    history_items = []
    # Find all .json files
    json_files = glob.glob(os.path.join(Config.TEMP_DATA_DIR, "*.json"))

    for j_file in json_files:
        try:
            with open(j_file, 'r', encoding='utf-8') as f:
                meta = json.load(f)

                # Format date string
                ts = meta.get("saved_at", 0)
                meta["date_str"] = time.strftime('%Y-%m-%d %H:%M', time.localtime(ts))

                # Ensure status exists for display logic
                if "status" not in meta:
                    meta["status"] = "finished"  # Default for old records

                history_items.append(meta)
        except Exception:
            continue

    # Sort: Newest first
    history_items.sort(key=lambda x: x.get("saved_at", 0), reverse=True)
    return history_items


def get_dataset_path(task_id):
    """
    Returns the full filesystem path to the CSV file if it exists.
    Used for file downloads.
    """
    path = os.path.join(Config.TEMP_DATA_DIR, f"{task_id}.csv")
    if os.path.exists(path):
        return path
    return None