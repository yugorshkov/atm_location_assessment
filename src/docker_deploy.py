from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer

docker_block = DockerContainer.load("atms")

docker_dep = Deployment(
    name="atms-flow-docker",
    flow_name='etl_flow',
    entrypoint="src/etl.py:etl_flow",
    path="/opt/prefect/flows",
    infrastructure=docker_block
)

if __name__ == "__main__":
    docker_dep.apply()
