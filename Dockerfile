from python:3.6

RUN pip install -e git+https://git@github.com/uc-cdis/wool.git#egg=wool

COPY pyproject.toml /gen3/pyproject.toml
WORKDIR /gen3
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -

COPY . /gen3
RUN cp -r src/indexclient/indexclient gen3/.
RUN poetry install -vv

RUN py.test -vv tests
