FROM python:3.8

WORKDIR /etltool/
COPY ./ /etltool/

#RUN pip3 install -e .
RUN pip3 install -r requirements.txt

#CMD ["etl2cdm"]