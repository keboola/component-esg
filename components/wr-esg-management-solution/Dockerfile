FROM python:3.11-slim
ENV PYTHONIOENCODING utf-8

RUN pip install flake8

COPY requirements.txt /code/requirements.txt
RUN pip install -r /code/requirements.txt

COPY /components/wr-esg-management-solution/src /code/src/
COPY /components/wr-esg-management-solution/tests /code/tests/
COPY /components/wr-esg-management-solution/scripts /code/scripts/
COPY flake8.cfg /code/flake8.cfg
COPY deploy.sh /code/deploy.sh

WORKDIR /code/

CMD ["python", "-u", "/code/src/component.py"]
