FROM python:3.8

WORKDIR /etltool
COPY . /etltool

RUN pip install -r requirements.txt

ENTRYPOINT ["python","etltool.py"]