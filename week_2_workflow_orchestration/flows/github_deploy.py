from prefect.deployments import Deployment
from prefect.filesystems import GitHub
from etl_web_to_gcs import etl_parent_flow

github_block = GitHub.load("zoom-git")

github_deploy = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name='GitHub-flow:Q5',
    parameters={"year":2019, "color":"yellow","months":[2,3]},
    storage=github_block,
    entrypoint="week_2_workflow_orchestration/flows/etl_web_to_gcs.py:etl_parent_flow"
)


if __name__ == '__main__':
    github_deploy.apply()
