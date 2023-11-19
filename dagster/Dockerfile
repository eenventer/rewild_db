FROM python:3.11.1-bullseye

WORKDIR /app

COPY ./requirements.txt /app
COPY ./dagster/dagster.yaml /app
COPY ./dagster/*.csv /app
COPY ./dagster/pyproject.toml /app
COPY ./dagster/rewild_dagster /app/rewild_dagster

RUN python -m pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 3000

# Set dagster home directory
ENV DAGSTER_HOME=/app
# Port to use for the Dagster webserver
ENV DAGIT_PORT=3000
# Listen on all interfaces
ENV DAGIT_HOST=0.0.0.0
# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["dagster", "dev"]