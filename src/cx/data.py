import datetime
from typing import List, Tuple
from src.log import logger
from .scan import get_query_counters


def get_latest_per_project(items: List[Tuple[str, str, str, str]]) -> List[Tuple[str, str, str, str]]:
    """
    获取每个project_id对应的created_at最大的元组

    参数:
        items: 元组列表，每个元组格式为 (scan_id, project_id, branch, created_at)
               其中created_at为RFC 3339格式时间字符串（如"2025-11-13T04:58:32.167Z"）

    返回:
        每个project_id对应的最新元组列表（按project_id首次出现顺序排列）
    """
    # 用字典存储每个project_id的最新元组，键为project_id，值为对应元组
    latest_map = {}

    for item in items:
        _, project_id, _, created_at = item  # 提取需要比较的字段
        # 如果该project_id未记录，或当前item的created_at更新，则替换
        if project_id not in latest_map or created_at > latest_map[project_id][3]:
            latest_map[project_id] = item

    # 转换为列表返回（保留字典插入顺序，Python 3.7+字典有序）
    return list(latest_map.values())


def get_date_list(number_of_days, base=None):
    if base is None:
        base = datetime.datetime.today()
    return [(base - datetime.timedelta(days=x)) for x in range(number_of_days)]


def get_cx_one_data_and_write_to_db(
        queries: List[str],
        severities: List[str],
        projects_scanned: List[tuple],
        db_connection,
        project_id_with_names: dict
):
    for project in projects_scanned:
        scan_id = project[0]
        project_id = project[1]
        branch = project[2]
        project_name = project_id_with_names[project_id]
        logger.info(
            f"HTTP call to get data for project id: {project_id} "
        )
        queries_counters = get_query_counters(scan_id=scan_id)
        logger.info(f"branch: {branch}")
        logger.info(
            f"Begin to write data into in-memory sqlite for project id: {project_id}, "
        )
        for result in queries_counters:
            query_name = result.get("queryName")
            if query_name == "No Results":
                continue
            if queries != "ALL" and query_name not in queries:
                continue
            result_severity = result.get("severity").lower()
            if result_severity not in severities:
                continue
            result_quantity = result.get("counter")
            with db_connection:
                db_connection.execute(
                    f"INSERT INTO results "
                    f"(PROJECT_ID, PROJECT_NAME, BRANCH, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY)"
                    f"VALUES (?,?,?,?,?,?) ON CONFLICT (PROJECT_ID, BRANCH, QUERY_NAME) "
                    f"DO UPDATE SET RESULT_QUANTITY = ?",
                    (project_id, project_name, branch, query_name, result_severity,
                     result_quantity,
                     result_quantity)
                )
        logger.info(f"finish write data "
                    f"for project id: {project_id}, "
                    f"project name: {project_name}, "
                    f"branch: {branch} "
                    f" into in-memory sqlite")
    logger.info("All data has been written into database")


def get_date_range(args: dict) -> tuple:
    range_type = args.get("range_type")
    date_format = "%Y-%m-%d-%H-%M-%S"

    calculated_date_range = []
    if range_type == "ALL":
        calculated_date_range = get_date_list(366)
    elif range_type == "PAST_DAY":
        calculated_date_range = get_date_list(2)
    elif range_type == "PAST_WEEK":
        calculated_date_range = get_date_list(8)
    elif range_type == "PAST_MONTH":
        calculated_date_range = get_date_list(31)
    elif range_type == "PAST_3_MONTH":
        calculated_date_range = get_date_list(91)
    elif range_type == "PAST_YEAR":
        calculated_date_range = get_date_list(366)
    elif range_type == "CUSTOM":
        date_from = datetime.datetime.strptime(args.get("date_from"), date_format)
        date_to = datetime.datetime.strptime(args.get("date_to"), date_format)
        day_delta = (date_to - date_from).days
        calculated_date_range = get_date_list(day_delta, date_to)
    start_date_time = calculated_date_range[-1]
    end_date_time = calculated_date_range[0]
    return end_date_time, start_date_time
