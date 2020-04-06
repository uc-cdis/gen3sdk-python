import os
import hashlib
import logging
from typing import List, Dict
from multiprocessing.pool import Pool


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
            (str, str): hash type and hash string
        """

        try:
            hash = getattr(hashlib, hash_type)()
        except AttributeError as e:
            logging.error("hashlib does not have {} as its function".format(hash_type))
            return None
        with open(self.file_path, "rb") as f:
            while True:
                chunk = bytearray(CHUNK_SIZE)
                num = f.readinto(chunk)
                if not num:
                    break
                hash.update(chunk[:num])
            return hash_type, hash.hexdigest()

    def get_hashes(self, hash_types: List[str]) -> Dict[str, str]:
        """
        Get multiple hashes with multiple processing

        Args:
            hash_types(list(str)): list of hash types
        
        Returns:
            Dict[str, str]: hash dictionary
                {
                    "md5": "md5_example",
                    "sha1": "sha1_example"
                }

        """
        number_of_workers = os.cpu_count()
        with Pool(number_of_workers) as pool:
            files_hash = pool.map(self.get_hash, hash_types)
            return dict(files_hash)
