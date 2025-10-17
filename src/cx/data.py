import datetime
from src.log import logger
from .project import get_projects
from .scan import get_query_counters


def get_date_list(number_of_days, base=None):
    if base is None:
        base = datetime.datetime.today()
    return [(base - datetime.timedelta(days=x)) for x in range(number_of_days)]


def get_cx_one_data_and_write_to_db(args, severities, db_connection):
    queries = args.get("queries")
    time_stamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    end_date_time, start_date_time = get_date_range(args)
    projects = get_projects()
    for project in projects:
        project_id = project.get("project_id")
        branches = project.get("branches")
        project_name = project.get("project_name")
        logger.info(f"HTTP call to get data "
                    f"for project id: {project_id}, "
                    f"project name: {project_name}, ")

        queries_counters, branch = get_query_counters(
            project_id=project_id,
            branches=branches,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            time_stamp_format=time_stamp_format
        )
        logger.info(f"branch: {branch}")
        logger.info(f"Begin to write data into in-memory sqlite"
                    f"for project id: {project_id}, "
                    f"project name: {project_name}, "
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
    start_date_time = datetime.datetime.strptime(calculated_date_range[-1].strftime("%Y-%m-%d-0-0-0"), date_format)
    end_date_time = datetime.datetime.strptime(calculated_date_range[0].strftime("%Y-%m-%d-23-59-59"), date_format)
    return end_date_time, start_date_time
