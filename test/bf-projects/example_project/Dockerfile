FROM python:3.7.15-slim
COPY ./dist /dist
RUN apt-get -y update && apt-get install -y libzbar-dev libc-dev musl-dev
RUN for i in /dist/*.whl; do pip install $i; done