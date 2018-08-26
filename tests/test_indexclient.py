
from gen3.indexclient.client import IndexClient


def test_index_list():
    indexHost = "https://nci-crdc-demo.datacommons.io/index/"
    indexVersion = ("v0",)
    indexAuth = ()

    ic = IndexClient(indexHost, indexVersion, indexAuth)
    doc = ic.get("001cf43a-40ed-4c45-8da4-0427d7a750dd")
    assert doc
