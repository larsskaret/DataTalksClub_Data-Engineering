FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

#Last, so pip dont use anything cached
RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data

#RUN mkdir /opt/prefect/data/yellow