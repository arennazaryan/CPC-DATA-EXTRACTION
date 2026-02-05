import traceback
from core.cpc_data_collector import CpcDataCollector
from .task_manager import task_manager


class CollectorService:
    @staticmethod
    def run_collection_task(task_id, form_data):
        try:
            row_name = form_data.get('row_name')
            year = int(form_data.get('year'))
            limit = int(form_data.get('limit'))

            # Optional filters
            declarant_type = form_data.get('declarant_type')
            t_type = form_data.get('type')
            inst_group = form_data.get('group')
            institution = form_data.get('institution')

            # Retry IDs (List of integers)
            retry_ids = form_data.get('retry_ids')

            if inst_group == "0": inst_group = None
            if institution == "0": institution = None

            stop_event = task_manager.get_stop_event(task_id)

            def progress_callback(current, total):
                task_manager.update_progress(task_id, current, total)

            collector = CpcDataCollector(
                row_name=row_name,
                year=year,
                declarant_type=declarant_type,
                t_type=t_type,
                inst_group=inst_group,
                institution=institution,
                limit=limit,
                progress_callback=progress_callback,
                stop_event=stop_event,
                retry_ids=retry_ids  # Pass this new param
            )

            collector.get_declarations()
            collector.get_row_data()
            collector.get_values()

            final_data, failed_items = collector.merge_and_save()

            task_manager.complete_task(task_id, {
                'data': final_data,
                'failed_ids': failed_items,
                'total_records': len(final_data)
            })

        except Exception as e:
            print(traceback.format_exc())
            task_manager.fail_task(task_id, str(e))
