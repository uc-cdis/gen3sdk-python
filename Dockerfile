from python:3.6

RUN pip install -e git+https://git@github.com/uc-cdis/wool.git#egg=wool

COPY requirements.txt /gen3/requirements.txt
COPY test-requirements.txt /gen3/test-requirements.txt
WORKDIR /gen3
RUN pip install -r requirements.txt \
    && pip install -r test-requirements.txt

COPY . /gen3
RUN cp -r src/indexclient/indexclient gen3/.
RUN python setup.py install

RUN py.test -vv tests
