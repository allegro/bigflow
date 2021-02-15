# Docker base image - change to upgrade python version.
FROM python:3.7.9-slim-buster

# Working directory.
WORKDIR /app

# Install apt packages.
RUN apt-get update && apt-get install -y \
  libzbar-dev \
  libc-dev \
  musl-dev \
  && rm -rf /var/lib/apt/lists/*

# Preinstall python packages (docker layer caching)
COPY ./resources/requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Install bigflow project 'whl' package.
COPY ./dist dist
RUN pip install dist/*.whl
