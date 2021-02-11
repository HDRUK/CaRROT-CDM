FROM python:3.8

WORKDIR /etltool/
COPY ./requirements.txt /etltool/
COPY ./cdm/ /etltool/cdm/

RUN pip install -r requirements.txt

COPY ./etltool.py /etltool/

ENTRYPOINT ["python","etltool.py"]