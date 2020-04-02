import pytest
import asyncio

from gen3.hash import Gen3Hash

def _create_file_with_string(filename, s):
    with open(filename, 'w') as writer:
        writer.write(s)

def test_hash():
    _create_file_with_string("./test.data", "This is a test")
    gen3Hash = Gen3Hash("./test.data")
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gen3Hash.get_hash_async(["md5", "sha1"], queue))
    
    L = []
    for _ in range(queue.qsize()):
        L.append(queue.get_nowait())
        queue.task_done()
    L.sort(key=lambda  x: str(x))

    assert L[0]['md5'] == 'ce114e4501d2f4e2dcea3e17b546f339'
    assert L[1]['sha1'] == 'a54d88e06612d820bc3be72877c74f257b561b19'
