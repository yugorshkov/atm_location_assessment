from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from etl import main

docker_block = DockerContainer.load("atms")

docker_dep = Deployment.build_from_flow(
    flow=main,
    name="atms-docker-flow",
    infrastructure=docker_block,
)

if __name__ == "__main__":
    docker_dep.apply()
