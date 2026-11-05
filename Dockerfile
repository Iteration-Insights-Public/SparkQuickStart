# Dockerfile
FROM python:3.11.8-slim

# ---- OS deps (Java for Spark) ----
# RUN apt-get update \
#  && apt-get install -y --no-install-recommends openjdk-21-jre-headless curl ca-certificates tini gcc build-essential bash wget\
#  && rm -rf /var/lib/apt/lists/*

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jdk curl ca-certificates tini gcc build-essential bash wget git \
    && rm -rf /var/lib/apt/lists/*


# ---- Speed up & standardize installs ----
WORKDIR /opt/app
# requirements.txt is a lock compiled from pyproject.toml — see its header.
# Copied on its own so this layer caches on the dependency set, not on source.
COPY requirements.txt requirements.txt

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Useful defaults for CI/local runs — set before the Delta warm-up below, so the JVM
# it starts binds exactly as the test-time one does.
ENV PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    SPARK_LOCAL_IP=127.0.0.1 \
    PYTHONUNBUFFERED=1

# Resolve the Delta JARs into the image's ivy cache now, in a layer keyed on the lock, so
# tests/conftest.py's spark.jars.packages resolves offline at test time. CI runs the tests
# with --network none: without this layer every run fetched them from Maven, and an
# outage made the Delta tests skip (F39, left open by F30).
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

# App workspace (your compose will mount the repo here)
WORKDIR /app

# ADD ./src /app/src
# ADD ./tests /app/tests
ADD ./ /app

# Install the package so spark_quick_start is importable by name
RUN pip install --no-cache-dir -e .

# Coverage output lands in /app/htmlcov. Mount a host directory to persist it:
#   docker run --rm -v $(pwd)/htmlcov:/app/htmlcov <image>
# or add a volume in docker-compose.yml:
#   volumes:
#     - ./htmlcov:/app/htmlcov
VOLUME ["/app/htmlcov"]

# Use tini as PID 1 for clean signal handling in compose
ENTRYPOINT ["/usr/bin/tini", "--", "bash", "-lc"]

# Default: run the full test suite with coverage (matches pytest.ini settings)
CMD ["pytest"]
