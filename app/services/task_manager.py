import threading
import time
import uuid
import traceback
from app.services.cache_service import save_dataset
from core.cpc_data_collector import CpcDataCollector

TASKS = {}
task_semaphore = threading.BoundedSemaphore(value=2)


def start_collection_task(filters):
    if not task_semaphore.acquire(blocking=False):
        raise Exception("Server is busy (Max 2 tasks running). Please try again in a few minutes.")

    try:
        task_id = str(uuid.uuid4())
        stop_event = threading.Event()

        TASKS[task_id] = {
            "status": "initializing",
            "progress": 0,
            "total": 0,
            "start_time": time.time(),
            "stop_event": stop_event,
            "message": "Starting collection..."
        }

        thread = threading.Thread(target=_worker, args=(task_id, filters, stop_event))
        thread.daemon = True
        thread.start()

        return task_id

    except Exception as e:
        task_semaphore.release()
        raise e


def _worker(task_id, filters, stop_event):
    def progress_callback(completed, total):
        if task_id in TASKS:
            TASKS[task_id]["progress"] = completed
            TASKS[task_id]["total"] = total
            TASKS[task_id]["status"] = "processing"
            TASKS[task_id]["message"] = f"Processed {completed} of {total} items..."

    try:
        print(f"[{task_id}] Starting Worker...")

        def safe_int(key, default=None):
            val = filters.get(key)
            if val is None:
                return default
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        collector = CpcDataCollector(
            row_name=filters["row_name"],
            year=safe_int("year", 2026),
            declarant_type=safe_int("declarant_type"),
            t_type=safe_int("t_type"),
            inst_group=safe_int("inst_group"),
            institution=safe_int("institution"),
            offset=safe_int("offset", 0),
            limit=safe_int("limit", 100),
            progress_callback=progress_callback,
            stop_event=stop_event,
            retry_ids=filters.get("retry_ids")
        )

        TASKS[task_id]["message"] = "Fetching initial declarations list..."
        collector.get_declarations()

        if not collector.declaration_list:
            TASKS[task_id]["status"] = "finished"
            TASKS[task_id]["message"] = "No declarations found."
            metadata = {"filters": filters, "total_declarations": 0, "total_rows": 0, "status": "finished"}
            save_dataset(task_id, [], metadata, [])
            return

        TASKS[task_id]["total"] = len(collector.declaration_list)
        TASKS[task_id]["message"] = "Downloading detailed row data..."

        collector.get_row_data()

        if stop_event.is_set():
            TASKS[task_id]["status"] = "stopped"
            TASKS[task_id]["message"] = "Process stopped by user."
            collector.get_values()
            final_data, failed_items = collector.merge_and_save()
            metadata = {"filters": filters, "total_declarations": len(collector.declaration_list),
                        "total_rows": len(final_data), "stopped_early": True, "status": "stopped"}
            save_dataset(task_id, final_data, metadata, failed_items)
            return

        TASKS[task_id]["message"] = "Formatting data..."
        collector.get_values()
        final_data, failed_items = collector.merge_and_save()

        metadata = {"filters": filters, "total_declarations": len(collector.declaration_list),
                    "total_rows": len(final_data), "status": "finished"}
        save_dataset(task_id, final_data, metadata, failed_items)

        TASKS[task_id]["status"] = "finished"
        TASKS[task_id]["message"] = "Complete"

    except Exception as e:
        error_msg = str(e)
        if task_id in TASKS:
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["message"] = f"Error: {error_msg}"
        print(f"[{task_id}] CRITICAL ERROR: {error_msg}")
        traceback.print_exc()

    finally:
        def cleanup():
            TASKS.pop(task_id, None)

        threading.Timer(300.0, cleanup).start()

        try:
            task_semaphore.release()
        except ValueError:
            pass


def get_task_status(task_id):
    task = TASKS.get(task_id)
    if not task:
        return None
    return {
        "status": task["status"],
        "progress": task["progress"],
        "total": task["total"],
        "message": task["message"],
        "start_time": task["start_time"]
    }


def stop_task(task_id):
    if task_id in TASKS:
        TASKS[task_id]["stop_event"].set()
        TASKS[task_id]["status"] = "stopping"
        return True
    return False

class TaskManager:
    pass


task_manager = TaskManager()
