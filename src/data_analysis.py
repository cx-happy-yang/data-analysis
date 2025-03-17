from src.__version__ import __version__
from src.log import logger
from src.args import get_command_line_arguments
from src.db import create_db
from src.excel import create_xlsx_file
from src.cx import get_cx_one_data_and_write_to_db


if __name__ == '__main__':
    logger.info(f"data-analysis version {__version__}")
    logger.info("start to run data-analysis")
    severity_list = ["critical", "high", "medium", "low"]
    cli_args = get_command_line_arguments(severity_list)
    severity_list_from_arg = cli_args.get("severities")
    if severity_list_from_arg != "ALL":
        severity_list = severity_list_from_arg
    db_connection = create_db()
    get_cx_one_data_and_write_to_db(args=cli_args, severities=severity_list, db_connection=db_connection)
    create_xlsx_file(
        db_connection=db_connection,
        severities=severity_list,
        report_file_path=cli_args.get("report_file_path")
    )
    logger.info("data-analysis finish")
