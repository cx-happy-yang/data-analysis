"""
pyinstaller -y -F --clean data_analysis.py
"""

import sqlite3
from CheckmarxPythonSDK.CxPortalSoapApiSDK import get_pivot_data
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
db.execute("""CREATE TABLE IF NOT EXISTS results (TEAM_NAME VARCHAR, PROJECT_NAME VARCHAR, QUERY_NAME VARCHAR, 
RESULT_SEVERITY INTEGER, RESULT_QUANTITY INTEGER, PRIMARY KEY (TEAM_NAME, PROJECT_NAME, QUERY_NAME) )""")


def get_command_line_arguments():
    """

    Returns:
        Namespace
    """
    import argparse
    description = 'A simple command-line interface for CxSAST in Python.'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--cxsast_base_url', required=True, help="CxSAST base url, for example: https://localhost")
    parser.add_argument('--cxsast_username', required=True, help="CxSAST username")
    parser.add_argument('--cxsast_password', required=True, help="CxSAST password")
    parser.add_argument('--include_not_exploitable', default="False", required=True, help="true or false")
    parser.add_argument('--range_type', default="CUSTOM", required=True,
                        help="ALL, PAST_DAY, PAST_WEEK, PAST_MONTH, PAST_3_MONTH, PAST_YEAR, CUSTOM")
    parser.add_argument('--date_from', help="example: 2023-06-01-0-0-0")
    parser.add_argument('--date_to', help="example: 2023-06-30-0-0-0")
    parser.add_argument('--queries', default="ALL", help="example: Code_Injection,Stored_XSS")
    parser.add_argument('--severities', default="ALL", help="example: Critical,High,Medium,Low,Info")
    parser.add_argument('--renamed_high_to_critical', default="False", help="true or false")
    parser.add_argument('--report_file_path', help="report file path")
    arguments = parser.parse_known_args()
    arguments = arguments[0]
    range_type_list = ["ALL", "PAST_DAY", "PAST_WEEK", "PAST_MONTH", "PAST_3_MONTH", "PAST_YEAR", "CUSTOM"]
    if arguments.range_type not in range_type_list:
        raise ValueError(f"command line argument: range_type should be any one of the following:\n"
                         f"ALL, PAST_DAY, PAST_WEEK, PAST_MONTH, PAST_3_MONTH, PAST_YEAR, CUSTOM")

    severity_list = ["critical", "high", "medium", "low", "info"]

    if arguments.severities != "ALL" and \
            not set([item.lower() for item in arguments.severities.split(",")]).issubset(set(severity_list)):
        raise ValueError(f"command line argument: severity should be any combinations of the following:\n"
                         f"Critical, High, Medium, Low, Info")

    args = {
        "cxsast_base_url": arguments.cxsast_base_url,
        "cxsast_username": arguments.cxsast_username,
        "include_not_exploitable": False if arguments.include_not_exploitable.lower() == "false" else True,
        "range_type": arguments.range_type,
        "date_from": arguments.date_from,
        "date_to": arguments.date_to,
        "queries": arguments.queries,
        "severities": [item.lower() for item in arguments.severities.split(",")]
        if arguments.severities != "ALL" else "ALL",
        "renamed_high_to_critical": False if arguments.renamed_high_to_critical.lower() == "false" else True,
        "report_file_path": arguments.report_file_path
    }
    logger.info(args)
    return args


def get_data_by_api_and_write_to_db(args, severity_map):
    queries = args.get("queries")
    severities = args.get("severities")
    if queries != "ALL":
        queries = queries.split(",")
    result_severity_list = []
    if severities != "ALL":
        for se in severities:
            result_severity_list.append(severity_map.get(se))
    # Because the data is so large, it would be timeout to get week data or even month data.
    # To Get Past Month pivot data, I will have to break it into days, and send http request day by day.
    range_type = args.get("range_type")
    date_format = "%Y-%m-%d-%H-%M-%S"

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

    for day in calculated_date_range:
        logger.info(f"HTTP call to get this day {day.date()} data")
        pivot_data = get_pivot_data(
            pivot_view_client_type="ProjectsLastScan",
            include_not_exploitable=args.get("include_not_exploitable"),
            range_type="CUSTOM",
            date_from=day.strftime("%Y-%m-%d-0-0-0"),
            date_to=day.strftime("%Y-%m-%d-23-59-59")
        )
        logger.info(f"Successfully get this day {day.date()} data")

        if pivot_data.PivotTable.Rows is None:
            logger.info(f"This day {day.date()} data is empty")
            continue

        logger.info(f"Begin to write {day.date()} data into in-memory sqlite")
        for row in pivot_data.PivotTable.Rows.CxPivotRow:
            value = row["Data"]["anyType"]
            team_name = value[0]
            project_name = value[1]
            query_name = value[2]
            if query_name == "No Results":
                continue
            if queries != "ALL" and query_name not in queries:
                continue
            result_severity = value[3]
            if severities != "ALL" and result_severity not in result_severity_list:
                continue
            result_quantity = value[6]
            with db:
                db.execute(f"INSERT INTO results "
                           f"(TEAM_NAME, PROJECT_NAME, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY)"
                           f"VALUES (?,?,?,?,?) ON CONFLICT (TEAM_NAME, PROJECT_NAME, QUERY_NAME) "
                           f"DO UPDATE SET RESULT_QUANTITY = ?",
                           (team_name, project_name, query_name, result_severity, result_quantity, result_quantity))
        logger.info(f"finish write {day.date()} data into in-memory sqlite")
    logger.info("All data has been written into database")


def create_xlsx_file(severity_list, severity_map, report_file_path):
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
    worksheet.freeze_panes(0, 1)  # Freeze the first column.
    # query_dict record the query: column information
    query_dict = {}
    # project_dict record the team_project: row information
    team_project_dict = {}
    severity_written_list = []

    def get_largest_value_of_a_dict(x):
        values = list(x.values())
        if len(values) == 0:
            return 1
        return sorted(values)[-1]

    def write_data_by_severity(se):
        severity_value = severity_map.get(se)
        sql_count = f"SELECT COUNT(DISTINCT QUERY_NAME) FROM results WHERE RESULT_SEVERITY = {severity_value}"
        sql_query = f"SELECT DISTINCT QUERY_NAME FROM results WHERE RESULT_SEVERITY = {severity_value} " \
                    f"ORDER BY QUERY_NAME ASC"
        sql_data = f"SELECT TEAM_NAME, PROJECT_NAME, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY FROM results " \
                   f"WHERE RESULT_SEVERITY = {severity_value} " \
                   f"ORDER BY TEAM_NAME ASC, PROJECT_NAME ASC, RESULT_SEVERITY DESC, QUERY_NAME ASC "
        column_index = get_largest_value_of_a_dict(query_dict)
        row_index = get_largest_value_of_a_dict(team_project_dict)
        with db:
            number_of_query = db.execute(sql_count).fetchone()[0]
            if number_of_query > 0:
                # write title
                column_index_start = column_index + 1
                if column_index == 1:
                    column_index_start = 1
                column_index = column_index_start
                column_index_end = column_index_start + number_of_query - 1
                total_column_index = column_index_end + 1
                worksheet.merge_range(0, column_index_start, 0, column_index_end, se, title_format)
                worksheet.merge_range(0, total_column_index, 1, total_column_index, se + ' total', title_format)
                query_dict.update({se + ' Total': total_column_index})
                for row in db.execute(sql_query):
                    worksheet.write(1, column_index, row[0], title_format)
                    query_dict.update({row[0]: column_index})
                    column_index += 1
                # write data
                for row in db.execute(sql_data):
                    team_name = row[0]
                    project_name = row[1]
                    query_name = row[2]
                    result_quantity = int(row[4])
                    current_team_project = team_name + "_" + project_name
                    row_number = team_project_dict.get(current_team_project)

                    if row_number is None:
                        row_index += 1
                        row_number = row_index
                        team_project_dict.update({current_team_project: row_number})
                        worksheet.write(row_index, 0, project_name, title_format)
                    row_number = team_project_dict.get(current_team_project)
                    if se + " Total" not in list(team_project_dict.keys()):
                        row_for_total = row_number + 1
                        column_start_letter = xl_col_to_name(column_index_start)
                        column_end_letter = xl_col_to_name(column_index_end)
                        func = f"=SUM({column_start_letter}{row_for_total}:" \
                               f"{column_end_letter}{row_for_total})"
                        logger.debug(f"{se} Total: row number {row_number} , column number: {total_column_index}, "
                                     f"func: {func}")
                        worksheet.write_formula(row_number, total_column_index, func)
                    worksheet.write_number(
                        row_number, query_dict.get(query_name), result_quantity
                    )
        severity_written_list.append(se)

    for severity in severity_list:
        logger.info(f"write severity {severity} data")
        write_data_by_severity(se=severity)
    worksheet.autofit()
    workbook.close()
    logger.info("finish creating Pivot.xlsx file")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    cli_args = get_command_line_arguments()
    renamed_high_to_critical = cli_args.get("renamed_high_to_critical")
    severity_dict = {
        "high": "3",
        "medium": "2",
        "low": "1",
        "info": "0",
    }
    if renamed_high_to_critical:
        severity_dict = {
            "critical": "3",
            "high": "2",
            "medium": "1",
            "low": "0",
        }
    get_data_by_api_and_write_to_db(cli_args, severity_dict)
    severity_list = cli_args.get("severities")
    if severity_list == "ALL":
        severity_list = severity_dict.keys()
    create_xlsx_file(
        severity_list=severity_list,
        severity_map=severity_dict,
        report_file_path=cli_args.get("report_file_path")
    )
