import pytest

from gen3.wss import wsurl_to_tokens


def test_wsurl_to_tokens():
    for it in [ 
        ("ws:///@user/abc/def", ("@user", "abc/def")),
        ("ws:///@whatever/abc", ("@whatever", "abc")) 
        ]:
        assert it[1] == wsurl_to_tokens(it[0])

