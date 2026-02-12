import datetime
from typing import List, Tuple
from src.log import logger
from CheckmarxPythonSDK.CxOne import (
    get_last_scan_info,
    get_summary_for_many_scans,
    get_sast_results_by_scan_id,
    get_a_list_of_scans,
)
from CheckmarxPythonSDK.CxOne.dto import (
    Scan,
    SastResult,
)


def get_all_scans_within_date_range(
        time_stamp_format: str,
        from_date: datetime.datetime,
        to_date: datetime.datetime
) -> List[Scan]:
    from_date = from_date.strftime(time_stamp_format)
    to_date = to_date.strftime(time_stamp_format)
    offset = 0
    limit = 500
    page = 1
    scans_collection = get_a_list_of_scans(
        offset=offset, limit=limit, from_date=from_date, to_date=to_date, sort=["-created_at"]
    )
    total_count = scans_collection.filtered_total_count
    scans = scans_collection.scans
    if total_count > limit:
        while True:
            offset = page * limit
            if offset >= total_count:
                break
            scans_collection = get_a_list_of_scans(
                offset=offset, limit=limit, from_date=from_date, to_date=to_date, sort=["-created_at"]
            )
            page += 1
            scans.extend(scans_collection.scans)
    return scans


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
        key=lambda r: datetime.datetime.strptime(list(r.values())[0].created_at, time_stamp_format),
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


def get_part_sast_results_by_scan_id(scan_id: str) -> Tuple[List[dict], int]:
    offset = 0
    limit = 500
    page = 1
    sast_results_collection = get_sast_results_by_scan_id(scan_id=scan_id, offset=offset, limit=limit, state=["TO_VERIFY"], include_nodes=False,)
    total_count = int(sast_results_collection.get("totalCount"))
    if total_count > 1000 or total_count == 0:
        logger.info(f"scan_id: {scan_id}, totalCount of SAST results is {total_count}, it is bigger than 1000 or equals to 0, will return an empty list []")
        return [], total_count
    sast_results = sast_results_collection.get("results")
    if total_count > limit:
        while True:
            offset = page * limit
            if offset >= total_count:
                break
            sast_results_collection = get_sast_results_by_scan_id(scan_id=scan_id, offset=offset, limit=limit, state=["TO_VERIFY"], include_nodes=False,)
            page += 1
            sast_results.extend(sast_results_collection.get("results"))
    if len(sast_results) < 10:
        logger.info(f"scan_id: {scan_id}, result length is less than 10, result: {sast_results}")
    statistics = calculate_statistics_of_sast_results(sast_results=sast_results)
    return statistics, len(sast_results)


def get_query_counters(
       scan_id: str
) -> List[dict]:
    result = []
    statistics_from_sast_results, total_count = get_part_sast_results_by_scan_id(scan_id=scan_id)
    if total_count == 0:
        logger.info(f"scan_id: {scan_id}, totalCount of SAST results is 0, will return an empty list []")
        result = []
    elif total_count > 0 and total_count <= 1000:
        result = statistics_from_sast_results
        if len(result) < 10:
            logger.info(f"scan_id: {scan_id}, result length is less than 10, result: {result}")
    else:
        logger.info(f"scan_id: {scan_id}, totalCount of SAST results is greater than 1000, will get query counters from scan summary")
        scan_summary = get_summary_for_many_scans(scan_ids=[scan_id], include_queries=True)
        scan_summaries = scan_summary.get("scansSummaries")
        if scan_summaries:
            queries_counters = scan_summaries[0].sast_counters.get("queriesCounters")
            if queries_counters:
                result = queries_counters
    return result
