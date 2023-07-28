from prefect.infrastructure.docker import DockerContainer

docker_block = DockerContainer(
    image="laggerkrd/atms:prefect",
    image_pull_policy="ALWAYS",
    auto_remove=True,
)

if __name__ == "__main__":
    docker_block.save("docker-atms", overwrite=True)
