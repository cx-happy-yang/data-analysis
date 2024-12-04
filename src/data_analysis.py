"""
pyinstaller -y -F --clean data_analysis.py
"""

import sqlite3
from CheckmarxPythonSDK.CxOne import (
    get_a_list_of_projects,
    get_branches,
    get_last_scan_info,
    get_summary_for_many_scans,
)
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
import logging
import pathlib
import datetime

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
db = sqlite3.connect(":memory:")
db.execute("""CREATE TABLE IF NOT EXISTS results (PROJECT_ID VARCHAR, PROJECT_NAME VARCHAR, BRANCH VARCHAR,
QUERY_NAME VARCHAR, RESULT_SEVERITY VARCHAR, RESULT_QUANTITY INTEGER, PRIMARY KEY (PROJECT_ID, BRANCH, QUERY_NAME) )""")
severity_list = ["critical", "high", "medium", "low"]


def get_command_line_arguments():
    """

    Returns:
        Namespace
    """
    import argparse
    description = 'A simple command-line interface for CxSAST in Python.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--cxone_access_control_url', required=True, help="CxOne iam url")
    parser.add_argument('--cxone_server', required=True, help="CxOne server url")
    parser.add_argument('--cxone_tenant_name', required=True, help="CxOne tenant name")
    parser.add_argument('--cxone_grant_type', required=True, help="CxOne grant type, refresh_token")
    parser.add_argument('--cxone_refresh_token', required=True, help="CxOne API Key")
    parser.add_argument('--cxone_proxy', help="proxy URL")
    parser.add_argument('--include_not_exploitable', default="False", required=True, help="true or false")
    parser.add_argument('--range_type', default="CUSTOM", required=True,
                        help="ALL, PAST_DAY, PAST_WEEK, PAST_MONTH, PAST_3_MONTH, PAST_YEAR, CUSTOM")
    parser.add_argument('--date_from', help="example: 2023-06-01-0-0-0")
    parser.add_argument('--date_to', help="example: 2023-06-30-0-0-0")
    parser.add_argument('--queries', default="ALL", help="example: Code_Injection,Stored_XSS")
    parser.add_argument('--severities', default="ALL", help="example: Critical,High,Medium,Low,Info")
    parser.add_argument('--report_file_path', help="report file path")
    arguments = parser.parse_known_args()
    arguments = arguments[0]
    range_type_list = ["ALL", "PAST_DAY", "PAST_WEEK", "PAST_MONTH", "PAST_3_MONTH", "PAST_YEAR", "CUSTOM"]
    if arguments.range_type not in range_type_list:
        raise ValueError(f"command line argument: range_type should be any one of the following:\n"
                         f"ALL, PAST_DAY, PAST_WEEK, PAST_MONTH, PAST_3_MONTH, PAST_YEAR, CUSTOM")

    if arguments.severities != "ALL" and \
            not set([item.strip().lower() for item in arguments.severities.split(",")]).issubset(set(severity_list)):
        raise ValueError(f"command line argument: severity should be any combinations of the following:\n"
                         f"Critical, High, Medium, Low")

    args = {
        "cxone_access_control_url": arguments.cxone_access_control_url,
        "cxone_server": arguments.cxone_server,
        "cxone_tenant_name": arguments.cxone_tenant_name,
        "cxone_grant_type": arguments.cxone_grant_type,
        "cxone_refresh_token": arguments.cxone_refresh_token,
        "cxone_proxy": arguments.cxone_proxy,
        "include_not_exploitable": False if arguments.include_not_exploitable.lower() == "false" else True,
        "range_type": arguments.range_type,
        "date_from": arguments.date_from,
        "date_to": arguments.date_to,
        "queries": arguments.queries.split(",") if arguments.queries != "ALL" else "ALL",
        "severities": [item.lower() for item in
                       arguments.severities.split(",")] if arguments.severities != "ALL" else "ALL",
        "report_file_path": arguments.report_file_path
    }
    logger.info(args)
    return args


def get_data_by_api_and_write_to_db(args, severities):
    queries = args.get("queries")
    range_type = args.get("range_type")
    date_format = "%Y-%m-%d-%H-%M-%S"
    time_stamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    def get_date_list(number_of_days, base=None):
        if base is None:
            base = datetime.datetime.today()
        return [(base - datetime.timedelta(days=x)) for x in range(number_of_days)]

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

    offset = 0
    limit = 100
    page = 1
    project_collection = get_a_list_of_projects(offset=offset, limit=limit)
    total_count = int(project_collection.totalCount)
    projects = project_collection.projects
    if total_count > limit:
        while True:
            offset = page * limit
            if offset >= total_count:
                break
            project_collection = get_a_list_of_projects(offset=offset, limit=limit)
            page += 1
            projects.extend(project_collection.projects)
    project_id_names = {}
    for project in projects:
        project_id_names.update({project.id: project.name})
    project_branches = {}
    for project_id in project_id_names.keys():
        project_branches.update({project_id: get_branches(limit=2048, project_id=project_id)})
    for project_id, branches in project_branches.items():
        if not branches:
            logger.info(f"{project_id} has no branches!")
            continue
        project_name = project_id_names.get(project_id)
        for branch in branches:
            logger.info(f"HTTP call to get data "
                        f"for project id: {project_id}, "
                        f"project name: {project_name}, "
                        f"branch: {branch} ")

            last_scan_map = get_last_scan_info(project_ids=[project_id], branch=branch)
            last_scan = last_scan_map.get(project_id)
            if not last_scan:
                continue
            scan_update_date_time = datetime.datetime.strptime(last_scan.updatedAt, time_stamp_format)
            if start_date_time > scan_update_date_time or end_date_time < scan_update_date_time:
                logger.info("the last scan from this project is not within the date range you specified! Will ignore!")
                continue
            scan_id = last_scan.id
            scan_summary = get_summary_for_many_scans(scan_ids=[scan_id], include_queries=True)
            scan_summaries = scan_summary.get("scansSummaries")
            if not scan_summaries:
                continue
            queries_counters = scan_summaries[0].sastCounters.get("queriesCounters")
            if not queries_counters:
                continue
            logger.info(f"Begin to write data into in-memory sqlite"
                        f"for project id: {project_id}, "
                        f"project name: {project_name}, "
                        f"branch: {branch} "
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
                with db:
                    db.execute(f"INSERT INTO results "
                               f"(PROJECT_ID, PROJECT_NAME, BRANCH, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY)"
                               f"VALUES (?,?,?,?,?,?) ON CONFLICT (PROJECT_ID, BRANCH, QUERY_NAME) "
                               f"DO UPDATE SET RESULT_QUANTITY = ?",
                               (project_id, project_name, branch, query_name, result_severity, result_quantity,
                                result_quantity))
            logger.info(f"finish write data "
                        f"for project id: {project_id}, "
                        f"project name: {project_name}, "
                        f"branch: {branch} "
                        f" into in-memory sqlite")
    logger.info("All data has been written into database")


def create_xlsx_file(severities, report_file_path):
    """
    create xlsx file based on the data, and following the same layout as the data analysis template.
    :return:
    """
    # Create a workbook and add a worksheet.
    logger.info("start creating Pivot.xlsx file")
    report_path = report_file_path
    if report_path is None:
        report_path = "./Pivot.xlsx"
    path = pathlib.Path(report_path)
    if path.is_dir():
        report_path += "/Pivot.xlsx"

    workbook = xlsxwriter.Workbook(report_path)
    worksheet = workbook.add_worksheet()
    worksheet.set_default_row(20)

    title_format = workbook.add_format({'align': 'left',
                                        'locked': True,
                                        'text_wrap': True,
                                        'border': 1,
                                        'bg_color': '#F0F0F0',
                                        'border_color': '#A0A0A0'})
    worksheet.merge_range('A1:A2', '')
    worksheet.freeze_panes(0, 2)  # Freeze the first column.
    # query_column_dict record the query: column information
    query_column_dict = {}
    # project_id_row_dict record the project_id: row information
    project_id_row_dict = {}
    severity_written_list = []
    content_start_index = 2

    def get_largest_value_of_a_dict(x):
        """
            a dict is key value pairs, this function will get the largest value from the dict values.
        """
        values = list(x.values())
        if len(values) == 0:
            return content_start_index
        return sorted(values)[-1]

    def write_data_by_severity(severity_value):
        sql_count = f"SELECT COUNT(DISTINCT QUERY_NAME) FROM results WHERE RESULT_SEVERITY = '{severity_value}'"
        sql_query = (f"SELECT DISTINCT QUERY_NAME FROM results WHERE RESULT_SEVERITY = '{severity_value}' "
                     f"ORDER BY QUERY_NAME ASC")
        sql_data = (f"SELECT PROJECT_ID, PROJECT_NAME, BRANCH, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY "
                    f"FROM results "
                    f"WHERE RESULT_SEVERITY = '{severity_value}' "
                    f"ORDER BY PROJECT_ID ASC, RESULT_SEVERITY DESC, QUERY_NAME ASC ")
        column_index = get_largest_value_of_a_dict(query_column_dict)
        with db:
            number_of_query = db.execute(sql_count).fetchone()[0]
            if number_of_query == 0:
                logger.info(f"no queries found from in-memory database for severity {severity_value}")
                return
            # write title
            column_index_start = column_index + 1
            if column_index == content_start_index:
                column_index_start = content_start_index
            column_index = column_index_start
            column_index_end = column_index_start + number_of_query - 1
            total_column_index = column_index_end + 1
            if not severity_written_list:
                worksheet.merge_range(0, 1, 1, 1, "Branch", title_format)
            worksheet.merge_range(0, column_index_start, 0, column_index_end, severity_value, title_format)
            worksheet.merge_range(0, total_column_index, 1, total_column_index, severity_value + ' total', title_format)
            query_column_dict.update({severity_value + " Total": total_column_index})
            for row in db.execute(sql_query):
                query_name = row[0]
                worksheet.write(1, column_index, query_name, title_format)
                query_column_dict.update({query_name: column_index})
                column_index += 1
            # write data
            for row in db.execute(sql_data):
                project_id = row[0]
                project_name = row[1]
                branch = row[2]
                query_name = row[3]
                result_quantity = int(row[5])
                row_number = project_id_row_dict.get(project_id)
                row_index = get_largest_value_of_a_dict(project_id_row_dict)
                if row_number is None:
                    if not project_id_row_dict:
                        row_number = content_start_index
                    else:
                        row_index += 1
                        row_number = row_index
                    project_id_row_dict.update({project_id: row_number})
                    worksheet.write(row_index, 0, project_name, title_format)
                    worksheet.write(row_index, 1, branch, title_format)
                column_start_letter = xl_col_to_name(column_index_start)
                column_end_letter = xl_col_to_name(column_index_end)
                func = f"=SUM({column_start_letter}{row_number + 1}:" \
                       f"{column_end_letter}{row_number + 1})"
                logger.debug(f"{severity_value} Total: row number {row_number} , "
                             f"column number: {total_column_index}, "
                             f"func: {func}")
                worksheet.write_formula(row_number, total_column_index, func)
                worksheet.write_number(
                    row_number, query_column_dict.get(query_name), result_quantity
                )
        severity_written_list.append(severity_value)

    for severity in severities:
        logger.info(f"write severity {severity} data")
        write_data_by_severity(severity_value=severity)
    worksheet.autofit()
    workbook.close()
    logger.info("finish creating Pivot.xlsx file")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cli_args = get_command_line_arguments()
    severity_list_from_arg = cli_args.get("severities")
    if severity_list_from_arg != "ALL":
        severity_list = severity_list_from_arg
    get_data_by_api_and_write_to_db(cli_args, severity_list)
    create_xlsx_file(
        severities=severity_list,
        report_file_path=cli_args.get("report_file_path")
    )
