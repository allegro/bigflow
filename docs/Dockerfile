FROM python:3.7
COPY ./dist /dist
RUN apt-get -y update && apt-get install -y libzbar-dev libc-dev musl-dev
RUN pip install pip==20.2.4
RUN for i in /dist/*.whl; do pip install $i --use-feature=2020-resolver; done