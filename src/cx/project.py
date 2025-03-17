from typing import List
from src.log import logger
from CheckmarxPythonSDK.CxOne import (
    get_a_list_of_projects,
    get_branches,
)


def extract_project_info_from_api_response(project_collection):
    return [
        {
            "project_id": project.id,
            "project_name": project.name,
            "branches": [],
        }
        for project in project_collection.projects
    ]


def get_projects() -> List[dict]:
    projects = []
    offset = 0
    limit = 100
    page = 1
    project_collection = get_a_list_of_projects(offset=offset, limit=limit)
    total_count = int(project_collection.totalCount)
    projects.extend(extract_project_info_from_api_response(project_collection))
    if total_count > limit:
        while True:
            offset = page * limit
            if offset >= total_count:
                break
            project_collection = get_a_list_of_projects(offset=offset, limit=limit)
            page += 1
            projects.extend(extract_project_info_from_api_response(project_collection))
    for project in projects:
        project["branches"] = get_branches(limit=2048, project_id=project.get("project_id"))
    results = []
    for project in projects:
        if project.get("branches"):
            results.append(project)
        else:
            logger.info(f"{project.get("project_name")} has no branches, ignore it")
    return results
