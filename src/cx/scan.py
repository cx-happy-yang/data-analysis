import datetime
from typing import List
from CheckmarxPythonSDK.CxOne import (
    get_last_scan_info,
    get_summary_for_many_scans,
)


def get_query_counters(
        project_id: str,
        branch: str,
        start_date_time,
        end_date_time,
        time_stamp_format
) -> List[dict]:
    result = []
    last_scan_map = get_last_scan_info(project_ids=[project_id], branch=branch)
    last_scan = last_scan_map.get(project_id)
    if last_scan:
        scan_update_date_time = datetime.datetime.strptime(last_scan.updatedAt, time_stamp_format)
        if start_date_time <= scan_update_date_time <= end_date_time:
            scan_id = last_scan.id
            scan_summary = get_summary_for_many_scans(scan_ids=[scan_id], include_queries=True)
            scan_summaries = scan_summary.get("scansSummaries")
            if scan_summaries:
                queries_counters = scan_summaries[0].sastCounters.get("queriesCounters")
                if queries_counters:
                    result = queries_counters
    return result
