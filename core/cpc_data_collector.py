import requests
import concurrent.futures
import json
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from app.config import Config

try:
    from core.row_config import rows_dict
except ImportError:
    from .row_config import rows_dict


class CpcDataCollector:
    def __init__(self, row_name, year, declarant_type, t_type,
                 inst_group, institution, offset=0, limit=100,
                 progress_callback=None, stop_event=None, retry_ids=None):

        self.row_name = row_name
        self.year = year
        self.declarant_type = declarant_type
        self.t_type = t_type
        self.inst_group = inst_group
        self.institution = institution
        self.offset = offset
        self.limit = min(limit, 100) if limit else 100

        self.progress_callback = progress_callback
        self.stop_event = stop_event
        self.retry_ids = retry_ids if retry_ids else []

        self.declaration_list = []
        self.declarant_ids = []
        self.rows_by_id = {}
        self.headers_by_id = {}
        self.final_data = []
        self.failed_items = []

        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.headers.update({"User-Agent": Config.USER_AGENT})

    def _safe_request(self, method, url, **kwargs):
        try:
            resp = self.session.request(method, url, timeout=Config.REQUEST_TIMEOUT, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            raise Exception(f"API Error {e.response.status_code}")
        except requests.exceptions.ConnectionError:
            raise Exception("Connection failed")
        except requests.exceptions.Timeout:
            raise Exception("Timeout")
        except Exception as e:
            raise Exception(str(e))

    def get_declarations(self):
        if self.stop_event and self.stop_event.is_set():
            return [], []

        base_url = f"{Config.CPC_BASE_URL}/declarations"

        api_filter = {"year": self.year}
        if self.declarant_type: api_filter["declarantType"] = self.declarant_type
        if self.t_type: api_filter["type"] = self.t_type
        if self.inst_group: api_filter["institutionGroup"] = self.inst_group
        if self.institution: api_filter["institution"] = self.institution

        payload = {
            "filter": api_filter,
            "paging": {"offset": self.offset, "limit": self.limit}
        }

        try:
            resp = self._safe_request("POST", base_url, json=payload)
            data = resp.json().get("data", [])

            if self.retry_ids:
                retry_set = set(map(int, self.retry_ids))
                data = [d for d in data if d["id"] in retry_set]

            self.declaration_list.extend(data)
            self.declarant_ids.extend([d["id"] for d in data])
            return self.declaration_list, self.declarant_ids

        except Exception as e:
            print(f"List Fetch Error: {e}")
            return [], []

    def _fetch_single_row(self, declarant_id, key_chain):
        # Even if stopped, we return a safe value here.
        # The main loop below handles the actual early exit.
        if self.stop_event and self.stop_event.is_set():
            return (declarant_id, None, "Stopped")

        url = f"{Config.CPC_BASE_URL}/declaration/{declarant_id}"

        try:
            resp = self._safe_request("GET", url)
            section = resp.json()
            for key in key_chain:
                key = int(key) if key.isdigit() else key
                section = section[key]
            return (declarant_id, section, None)

        except (KeyError, IndexError, TypeError):
            return (declarant_id, None, "Data section not found")
        except json.JSONDecodeError:
            return (declarant_id, None, "Invalid JSON")
        except Exception as e:
            return (declarant_id, None, str(e))

    def get_row_data(self):
        import re
        path_string = rows_dict.get(self.row_name)
        if not path_string: return {}

        key_chain = re.findall(r"\[['\"]?(.+?)['\"]?]", path_string)
        total = len(self.declarant_ids)
        if total == 0: return {}

        completed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_id = {
                executor.submit(self._fetch_single_row, d_id, key_chain): d_id
                for d_id in self.declarant_ids
            }

            # FIX: Non-blocking loop for instant stopping
            # We convert keys to a set so we can remove them as they complete
            futures_set = set(future_to_id.keys())

            while futures_set:
                # 1. Check Stop Signal IMMEDIATELY
                if self.stop_event and self.stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                # 2. Wait for at least one future to complete, but timeout every 0.5s to check stop again
                done, _ = concurrent.futures.wait(
                    futures_set,
                    timeout=0.5,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # 3. Process completed futures
                for future in done:
                    futures_set.remove(future)
                    declarant_id = future_to_id[future]

                    try:
                        _, result, error_reason = future.result()
                        if result is not None:
                            self.rows_by_id[declarant_id] = result
                        else:
                            self.failed_items.append({'id': declarant_id, 'reason': error_reason})
                    except Exception as e:
                        self.failed_items.append({'id': declarant_id, 'reason': str(e)})

                    completed += 1
                    if self.progress_callback:
                        self.progress_callback(completed, total)

        return self.rows_by_id

    def get_values(self):
        for declarant_id, section in self.rows_by_id.items():
            headers, rows = [], []
            if isinstance(section, dict) and "rows" in section:
                headers = [h["name"] for h in section.get("headerItems", [])]
                rows = section["rows"]
            elif isinstance(section, dict) and "cells" in section:
                for cell in section["cells"]:
                    v = cell.get("value")
                    if isinstance(v, dict) and "rows" in v:
                        headers = [h["name"] for h in v.get("headerItems", [])]
                        rows = v["rows"]
            elif isinstance(section, list):
                rows = section
            self.headers_by_id[declarant_id] = headers
            self.rows_by_id[declarant_id] = rows
        return self.rows_by_id, self.headers_by_id

    def _extract_row_values(self, row):
        if isinstance(row, list):
            headers = [f"col_{i + 1}" for i in range(len(row))]
            return headers, row
        if isinstance(row, dict) and "cells" in row:
            values = [c.get("value") for c in row["cells"]]
            headers = [c.get("title") or f"col_{i + 1}" for i, c in enumerate(row["cells"])]
            return headers, values
        return [], []

    def merge_and_save(self):
        final_data = []
        failed_id_set = {item['id'] for item in self.failed_items}

        for person in self.declaration_list:
            d_id = person["id"]
            if d_id in failed_id_set: continue

            rows = self.rows_by_id.get(d_id, [])
            headers = self.headers_by_id.get(d_id, [])

            if not rows:
                record = person.copy()
                for h in headers: record[h] = None
                final_data.append(record)
                continue

            for row in rows:
                row_headers, values = self._extract_row_values(row)
                eff_headers = headers or row_headers
                record = person.copy()
                for i, h in enumerate(eff_headers):
                    record[h] = values[i] if i < len(values) else None
                final_data.append(record)
        self.final_data = final_data
        return self.final_data, self.failed_items