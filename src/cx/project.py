from typing import List
from src.log import logger
from CheckmarxPythonSDK.CxOne import (
    get_a_list_of_projects,
)


def extract_project_info_from_api_response(project_collection):
    return {project.id: project.name for project in project_collection.projects}


def get_project_id_with_names(project_ids: List[str]) -> dict:
    logger.info("start to get all projects within the date range")
    results = {}
    chunk_size = 100
    for i in range(0, len(project_ids), chunk_size):
        chunk = project_ids[i:i+chunk_size]
        logger.info(f"send {i+1} round of request to get projects info, chunk size {len(chunk)}")
        offset = 0
        limit = 100
        page = 1
        project_collection = get_a_list_of_projects(offset=offset, limit=limit, ids=chunk)
        total_count = int(project_collection.filtered_total_count)
        results.update(extract_project_info_from_api_response(project_collection))
        if total_count > limit:
            while True:
                offset = page * limit
                if offset >= total_count:
                    break
                project_collection = get_a_list_of_projects(offset=offset, limit=limit, ids=chunk)
                page += 1
                results.update(extract_project_info_from_api_response(project_collection))
    logger.info(f"number of projects during the date range {len(results.keys())}")
    logger.info("finish get all projects within the date range")
    return results
