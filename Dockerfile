FROM python:3.6.8

RUN pip install pip --upgrade
RUN pip install co-connect-tools

WORKDIR /data/
COPY ./coconnect/data/test/inputs /data/inputs
COPY ./coconnect/data/test/rules /data/rules

