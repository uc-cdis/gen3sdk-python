import os
import hashlib
import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor


CHUNK_SIZE = 4 * 1024


class Gen3Hash:
    def __init__(self, file_path, chunk_size=CHUNK_SIZE):
        self.file_path = file_path
        self.chunk_size = chunk_size

    def get_hash(self, hash_type):
        """
        Compute hash of the object.

        Args:
            hash_type(str): the type of hash needs to be computed

        Returns:
            str: hash string
        """

        try:
            hash = getattr(hashlib, hash_type)()
        except AttributeError as e:
            logging.error("hashlib does not have {} as its function".format(hash_type))
            return None
        with open(self.file_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                hash.update(chunk)
            return hash.hexdigest()

    async def get_hash_async(self, hash_types, queue):
        """
        This function computes multiple hash types 

        Args:
            hash_types(list(str)): list of hash types
            queue(asyncio.Queue()): output queue

        """
        ec = ProcessPoolExecutor()

        for f in asyncio.as_completed(
            [
                loop.run_in_executor(ec, gen3Hash.get_hash, hash_type)
                for hash_type in hash_types
            ]
        ):
            hash = await f
            await queue.put(hash)
