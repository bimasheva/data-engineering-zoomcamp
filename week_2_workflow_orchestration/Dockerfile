FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

RUN mkdir /opt/prefect/flows
RUN mkdir /opt/prefect/data
RUN mkdir /opt/prefect/data/yellow
RUN mkdir /opt/prefect/data/green

COPY flows/etl_web_to_gcs.py /opt/prefect/flows/etl_web_to_gcs.py

