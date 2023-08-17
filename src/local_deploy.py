from prefect.deployments import Deployment
from etl import etl_flow

deployment = Deployment.build_from_flow(
    flow=etl_flow,
    name="atms_local", 
    work_queue_name="demo",
    work_pool_name="default-agent-pool",
)

if __name__ == "__main__":
    deployment.apply()
