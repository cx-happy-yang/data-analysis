from src.__version__ import __version__
from src.log import logger
from src.args import get_command_line_arguments
from src.db import create_db
from src.excel import create_xlsx_file
from src.cx import (
    get_date_range,
    get_latest_per_project,
    get_all_scans_within_date_range,
)
from src.cx.scan import get_query_counters


rfc_time_stamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"

if __name__ == '__main__':
    logger.info(f"data-analysis version {__version__}")
    logger.info("start to run data-analysis")
    severity_list = ["critical", "high", "medium", "low"]
    cli_args = get_command_line_arguments(severity_list)
    severity_list_from_arg = cli_args.get("severities")
    if severity_list_from_arg != "ALL":
        severity_list = severity_list_from_arg
    end_date_time, start_date_time = get_date_range(cli_args)
    queries = cli_args.get("queries")
    logger.info(f"date range, start: {start_date_time} end: {end_date_time}")
    all_scans_within_date_range = get_all_scans_within_date_range(
        time_stamp_format=rfc_time_stamp_format,
        from_date=start_date_time,
        to_date=end_date_time
    )
    logger.info(f"number of scans within the date range: {len(all_scans_within_date_range)}")
    accepted_branches = ["master", "release", "develop", "rc", "stage"]
    all_projects_scanned_within_date_range = list(set(
        [(
            scan.id, 
            scan.project_id, 
            scan.project_name, 
            scan.branch, 
            scan.created_at
        ) for scan in all_scans_within_date_range]
    ))
    all_latest_scans = get_latest_per_project(all_projects_scanned_within_date_range)
    logger.info(f"number of projects within the date range: {len(all_latest_scans)}")
    db_connection = create_db()
    try:
        for scan in all_latest_scans:
            scan_id = scan[0]
            project_id = scan[1]
            project_name = scan[2]
            branch = scan[3]
            logger.info(
                f"HTTP call to get data for" 
                f"scan id: {scan_id}, "
                f"project id: {project_id}, "
                f"project name: {project_name}, "
                f"branch: {branch} "
            )
            queries_counters = []
            try:
                queries_counters = get_query_counters(scan_id=scan_id)
            except Exception as e:
                logger.info(f"error: {e}")
                continue
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
                if result_severity not in severity_list:
                    continue
                result_quantity = result.get("counter")
                with db_connection:
                    db_connection.execute(
                        f"INSERT INTO results "
                        f"(SCAN_ID, PROJECT_ID, PROJECT_NAME, BRANCH, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY)"
                        f"VALUES (?,?,?,?,?,?,?) ON CONFLICT (PROJECT_ID, BRANCH, QUERY_NAME) "
                        f"DO UPDATE SET RESULT_QUANTITY = ?",
                        (scan_id, project_id, project_name, branch, query_name, result_severity,
                        result_quantity,
                        result_quantity)
                    )
            logger.info(f"finish write data "
                        f"scan id: {scan_id}, "
                        f"project id: {project_id}, "
                        f"project name: {project_name}, "
                        f"branch: {branch} "
                        f" into in-memory sqlite")
    except Exception as e:
        logger.info(f"error: {e}")
    else:
        logger.info("All data has been written into database")
    finally:
        create_xlsx_file(
            db_connection=db_connection,
            severities=severity_list,
            report_file_path=cli_args.get("report_file_path")
        )
    logger.info("data-analysis finish")
