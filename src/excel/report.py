import pathlib
import xlsxwriter
from xlsxwriter.utility import xl_col_to_name
from src.log import logger


def create_xlsx_file(db_connection, severities, report_file_path):
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
    worksheet.freeze_panes(0, 3)  # Freeze the first column.
    # query_column_dict record the query: column information
    query_column_dict = {}
    # project_id_row_dict record the project_id: row information
    project_id_row_dict = {}
    severity_written_list = []
    content_row_start_index = 2
    content_column_start_index = 3

    def get_largest_value_of_a_dict(x):
        """
            a dict is key value pairs, this function will get the largest value from the dict values.
        """
        values = list(x.values())
        if len(values) == 0:
            return content_column_start_index
        return sorted(values)[-1]

    def write_data_by_severity(severity_value):
        sql_count = f"SELECT COUNT(DISTINCT QUERY_NAME) FROM results WHERE RESULT_SEVERITY = '{severity_value}'"
        sql_query = (f"SELECT DISTINCT QUERY_NAME FROM results WHERE RESULT_SEVERITY = '{severity_value}' "
                     f"ORDER BY QUERY_NAME ASC")
        sql_data = (f"SELECT SCAN_ID, PROJECT_ID, PROJECT_NAME, BRANCH, QUERY_NAME, RESULT_SEVERITY, RESULT_QUANTITY "
                    f"FROM results "
                    f"WHERE RESULT_SEVERITY = '{severity_value}' "
                    f"ORDER BY PROJECT_ID ASC, RESULT_SEVERITY DESC, QUERY_NAME ASC ")
        column_index = get_largest_value_of_a_dict(query_column_dict)
        with db_connection:
            number_of_query = db_connection.execute(sql_count).fetchone()[0]
            if number_of_query == 0:
                logger.info(f"no queries found from in-memory database for severity {severity_value}")
                return
            # write title
            column_index_start = column_index + 1
            if column_index == content_column_start_index:
                column_index_start = content_column_start_index
            column_index = column_index_start
            column_index_end = column_index_start + number_of_query - 1
            total_column_index = column_index_end + 1
            if not severity_written_list:
                worksheet.merge_range(0, 1, 1, 1, "Branch", title_format)
                worksheet.merge_range(0, 2, 1, 2, "URL", title_format)
            if column_index_start != column_index_end:
                worksheet.merge_range(0, column_index_start, 0, column_index_end, severity_value, title_format)
            else:
                worksheet.write(0, column_index_start, severity_value, title_format)
            worksheet.merge_range(0, total_column_index, 1, total_column_index, severity_value + ' total', title_format)
            query_column_dict.update({severity_value + " Total": total_column_index})
            for row in db_connection.execute(sql_query):
                query_name = row[0]
                worksheet.write(1, column_index, query_name, title_format)
                query_column_dict.update({query_name: column_index})
                column_index += 1
            # write data
            for row in db_connection.execute(sql_data):
                scan_id = row[0]
                project_id = row[1]
                project_name = row[2]
                branch = row[3]
                query_name = row[4]
                result_quantity = int(row[6])
                url = f"https://sng.ast.checkmarx.net/sast-results/{project_id}/{scan_id}"
                row_number = project_id_row_dict.get(project_id)
                row_index = get_largest_value_of_a_dict(project_id_row_dict)
                if row_number is None:
                    if not project_id_row_dict:
                        row_number = content_row_start_index
                        row_index = content_row_start_index
                    else:
                        row_index += 1
                        row_number = row_index
                    project_id_row_dict.update({project_id: row_number})
                    worksheet.write(row_index, 0, project_name, title_format)
                    worksheet.write(row_index, 1, branch, title_format)
                    worksheet.write(row_index, 2, url, title_format)
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
