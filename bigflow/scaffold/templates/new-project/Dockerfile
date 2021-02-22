# Docker base image - change to upgrade python version.
FROM python:3.7.9-slim-buster

# Working directory.
WORKDIR /app

# Install binary system libraries and/or dev packages.
RUN apt-get update && apt-get install -y \
  # libc-dev \
  && rm -rf /var/lib/apt/lists/*

# Preinstall python packages (improve docker layer caching).
# This step is optional, but may improve docker image building time.
COPY ./resources/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install bigflow project 'whl' package.
COPY ./dist dist
RUN pip install dist/*.whl
