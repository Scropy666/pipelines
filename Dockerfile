FROM python:3.10 as base

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
WORKDIR /pipelines
RUN pip install --upgrade pip


FROM base as dep-poetry
ENV POETRY_HOME /opt/poetry
RUN python3 -m venv $POETRY_HOME
RUN $POETRY_HOME/bin/pip install poetry==1.2.2
ENV POETRY_BIN $POETRY_HOME/bin/poetry
COPY pyproject.toml poetry.lock ./
RUN $POETRY_BIN config --local virtualenvs.create false
RUN $POETRY_BIN install --no-root

COPY ./db /pipelines/db
COPY ./example_pipeline/norm /pipelines/example_pipeline/norm
COPY ./example_pipeline/pipeline.py /pipelines/example_pipeline/
COPY ./pipelines /pipelines/pipelines
COPY ./setup.py /pipelines
COPY ./README.md /pipelines
RUN pip install .

CMD ["python", "./example_pipeline/pipeline.py"]