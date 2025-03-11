FROM python:3.8.13 as base

ADD requirements.txt .
RUN pip uninstall protobuf
RUN pip install --no-binary protobuf protobuf
RUN pip3 install -r requirements.txt

ENV PYTHONPATH=$PYTHONPATH:/src/

ADD ./ /src/

WORKDIR /src

FROM base as build
ENTRYPOINT ["python3", "/src/model.py"]

RUN python3 -m spacy download en_core_web_sm