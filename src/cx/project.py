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
    logger.info("start to get all projects")
    projects = []
    offset = 0
    limit = 100
    page = 1
    project_collection = get_a_list_of_projects(offset=offset, limit=limit)
    total_count = int(project_collection.total_count)
    projects.extend(extract_project_info_from_api_response(project_collection))
    if total_count > limit:
        while True:
            offset = page * limit
            if offset >= total_count:
                break
            project_collection = get_a_list_of_projects(offset=offset, limit=limit)
            page += 1
            projects.extend(extract_project_info_from_api_response(project_collection))
    logger.info("finish get all projects")
    logger.info("start to get accepted branches")
    for project in projects:
        branches = get_branches(limit=1024, project_id=project.get("project_id"))
        if not branches:
            continue
        accepted_branches = ["master", "release", "develop", "rc", "stage"]
        project["branches"] = list(set(branches).intersection(set(accepted_branches)))
        logger.info(f"project: {project['project_name']} all accepted branches: {project['branches']}")
    logger.info("finish get accepted branches")
    results = []
    for project in projects:
        if project.get("branches"):
            results.append(project)
        else:
            logger.info(f"{project.get('project_name')} has no branches, ignore it")
    return results
