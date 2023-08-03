from prefect.infrastructure.container import DockerContainer

docker_block = DockerContainer(
    image="laggerkrd/atms:flow",
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

if __name__ == "__main__":
    docker_block.save("atms", overwrite=True)
