FROM python:3.8

WORKDIR /etltool/
COPY ./ /etltool/

RUN pip3 install -e .

#CMD ["etl2cdm"]