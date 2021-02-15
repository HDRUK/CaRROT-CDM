FROM python:3.8

WORKDIR /etltool/
COPY ./requirements.txt /etltool/

RUN pip install -r requirements.txt

COPY ./etltool.py /etltool/

RUN python etl2cdm.py 
CMD ["python","etl2cdm.py"]