import requests
import concurrent.futures
import json
import urllib3
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from app.config import Config
from core.row_config import rows_dict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        self.retry_ids: list = retry_ids if retry_ids else []

        self.declaration_list: list = []
        self.declarant_ids: list = []
        self.rows_by_id: dict = {}
        self.headers_by_id: dict = {}
        self.final_data: list = []
        self.failed_items: list = []

        self.session = requests.Session()
        self.session.verify = False

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
        except requests.exceptions.ConnectionError as e:
            print(f"Underlying Connection Error: {e}")
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

    @staticmethod
    def _safe_extract(data, key_chain):
        """Safely navigates a nested JSON object using a list of keys/indices."""
        current = data
        for key in key_chain:
            try:
                current = current[key]
            except (KeyError, IndexError, TypeError):
                return None
        return current

    def _fetch_single_row(self, declarant_id, key_chain):
        # 1. IMMEDIATE CHECK: If stopped, do not make any network requests or proceed
        if self.stop_event and self.stop_event.is_set():
            return declarant_id, None, "Stopped"

        url = f"{Config.CPC_BASE_URL}/declaration/{declarant_id}"

        try:
            resp = self._safe_request("GET", url)

            if self.stop_event and self.stop_event.is_set():
                return declarant_id, None, "Stopped"

            data = resp.json()
            section = self._safe_extract(data, key_chain)

            if section is None:
                return declarant_id, None, "Data section not found"
            return declarant_id, section, None

        except json.JSONDecodeError:
            return declarant_id, None, "Invalid JSON"
        except Exception as e:
            return declarant_id, None, str(e)

    def get_row_data(self):
        key_chain = rows_dict.get(self.row_name)
        if not key_chain:
            return {}

        total = len(self.declarant_ids)
        if total == 0:
            return {}

        completed = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            future_to_id = {
                executor.submit(self._fetch_single_row, d_id, key_chain): d_id
                for d_id in self.declarant_ids
            }

            futures_set = set(future_to_id.keys())

            while futures_set:
                if self.stop_event and self.stop_event.is_set():
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                done, _ = concurrent.futures.wait(
                    futures_set,
                    timeout=0.5,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                for future in done:
                    futures_set.remove(future)
                    declarant_id = future_to_id[future]

                    try:
                        _, result, error_reason = future.result()
                        if result is not None:
                            self.rows_by_id[declarant_id] = result
                        elif error_reason != "Stopped":
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
            if d_id in failed_id_set:
                continue

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
