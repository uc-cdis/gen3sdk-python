import tempfile
from gen3.hash import Gen3Hash


def test_hash():
    with tempfile.NamedTemporaryFile(delete=True) as fp:
        for _ in range(1000):
            fp.write(b"This is a test!")
        fp.flush()
        gen3Hash = Gen3Hash(fp.name)
        hash_dict = gen3Hash.get_hashes(["md5", "sha1"])
        assert hash_dict["md5"] == "8247353fad844ba637fac7288e17c962"
        assert hash_dict["sha1"] == "2ebbe8b7bf5373da3b82f29aebb7eb7cf6bf0db7"
