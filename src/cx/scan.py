import datetime
from typing import List
from CheckmarxPythonSDK.CxOne import (
    get_last_scan_info,
    get_summary_for_many_scans,
    get_sast_results_by_scan_id,
)
from CheckmarxPythonSDK.CxOne.dto import (
    SastResult
)


def get_last_scan_from_branches(project_id: str, branches: List[str], time_stamp_format: str) -> dict:
    last_scans = []
    if not branches:
        last_scans.append(get_last_scan_info(project_ids=[project_id]))
    for branch in branches:
        last_scan = get_last_scan_info(project_ids=[project_id], branch=branch)
        last_scans.append(last_scan)
    if not last_scans:
        return {}
    sorted_last_scans = sorted(
        last_scans,
        key=lambda r: datetime.datetime.strptime(list(r.values())[0].createdAt, time_stamp_format),
    )
    most_last_scan = sorted_last_scans[-1]
    return most_last_scan


def calculate_statistics_of_sast_results(sast_results: List[SastResult]) -> List[dict]:
    statistics = []
    for result in sast_results:
        if result.state != "TO_VERIFY":
            continue
        query_id = result.query_id_str
        target_dict = next((d for d in statistics if d.get("queryID") == query_id), None)
        if not target_dict:
            statistics.append({
                "counter": 1,
                "queryID": query_id,
                "queryName": result.query_name,
                "severity": result.severity
            })
        else:
            target_dict["counter"] += 1
    return statistics


def get_part_sast_results_by_scan_id(scan_id: str) -> List[dict]:
    offset = 0
    limit = 100
    page = 1
    sast_results_collection = get_sast_results_by_scan_id(scan_id=scan_id, offset=offset, limit=limit)
    total_count = int(sast_results_collection.get("totalCount"))
    if total_count > 500:
        return []
    sast_results = sast_results_collection.get("results")
    if total_count > limit:
        while True:
            offset = page * limit
            if offset >= total_count:
                break
            sast_results_collection = get_sast_results_by_scan_id(scan_id=scan_id, offset=offset, limit=limit)
            page += 1
            sast_results.extend(sast_results_collection.get("results"))
    statistics = calculate_statistics_of_sast_results(sast_results=sast_results)
    return statistics


def get_query_counters(
        project_id: str,
        branches: List[str],
        start_date_time,
        end_date_time,
        time_stamp_format
) -> (List[dict], str):
    result = []
    last_scan_map = get_last_scan_from_branches(
        project_id=project_id, branches=branches, time_stamp_format=time_stamp_format
    )
    last_scan = last_scan_map.get(project_id)
    if last_scan:
        scan_update_date_time = datetime.datetime.strptime(last_scan.updatedAt, time_stamp_format)
        if start_date_time <= scan_update_date_time <= end_date_time:
            scan_id = last_scan.id
            statistics_from_sast_results = get_part_sast_results_by_scan_id(scan_id=scan_id)
            if statistics_from_sast_results:
                result = statistics_from_sast_results
            else:
                scan_summary = get_summary_for_many_scans(scan_ids=[scan_id], include_queries=True)
                scan_summaries = scan_summary.get("scansSummaries")
                if scan_summaries:
                    queries_counters = scan_summaries[0].sastCounters.get("queriesCounters")
                    if queries_counters:
                        result = queries_counters
    return result, last_scan.branch
