from src.__version__ import __version__
from src.log import logger
from src.args import get_command_line_arguments
from src.db import create_db
from src.excel import create_xlsx_file
from src.cx import (
    get_cx_one_data_and_write_to_db,
    get_date_range,
    get_latest_per_project,
    get_all_scans_within_date_range,
    get_project_id_with_names,
)

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
        [(scan.id, scan.project_id, scan.branch, scan.created_at) for scan in all_scans_within_date_range]
    ))
    all_projects_with_latest_scan = get_latest_per_project(all_projects_scanned_within_date_range)
    logger.info(f"number of projects within the date range: {len(all_projects_with_latest_scan)}")
    project_id_with_names = get_project_id_with_names([item[1] for item in all_projects_with_latest_scan])
    db_connection = create_db()
    get_cx_one_data_and_write_to_db(
        queries=queries,
        severities=severity_list,
        projects_scanned=all_projects_with_latest_scan,
        db_connection=db_connection,
        project_id_with_names=project_id_with_names,
    )
    create_xlsx_file(
        db_connection=db_connection,
        severities=severity_list,
        report_file_path=cli_args.get("report_file_path")
    )
    logger.info("data-analysis finish")
