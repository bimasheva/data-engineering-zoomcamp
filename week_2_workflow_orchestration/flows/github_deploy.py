from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_parent_flow

github_block = GitHub.load("zoom-git")

github_deploy = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='GitHub-flow',
    parameters={"year":2020, "color":"green","months":[11]},
    storage=github_block,
    entrypoint="week_2_workflow_orchestration/flows/etl_web_to_gcs.py:etl_parent_flow"
)


if __name__ == '__main__':
    github_deploy.apply()
