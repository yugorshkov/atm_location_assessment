FROM prefecthq/prefect:2-python3.9
COPY docker-requirements.txt .
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir
RUN apt -y update && apt install wget && apt -y install osmium-tool
COPY src /opt/prefect/flows/src