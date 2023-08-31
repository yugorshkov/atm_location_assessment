from dotenv import dotenv_values
from prefect.infrastructure.container import DockerContainer

storage_credentials = dotenv_values(".env")

docker_block = DockerContainer(
    image="laggerkrd/atms:flow",
    image_pull_policy="ALWAYS",
    auto_remove=True,
    env=storage_credentials,
)

if __name__ == "__main__":
    docker_block.save("atms", overwrite=True)
