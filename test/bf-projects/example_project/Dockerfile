FROM python:3.8-slim
COPY ./dist /dist
RUN apt-get -y update && apt-get install -y libzbar-dev libc-dev musl-dev
RUN for i in /dist/*.whl; do pip install $i; done