from src.log import logger


def get_command_line_arguments(severity_list):
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
