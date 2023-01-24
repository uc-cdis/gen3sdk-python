"""
Classes for interacting with various NIH APIs.
"""
import requests
import xml.etree.ElementTree as ET
import os
import json
import queue


class dbgapStudyRegistration(object):
    """
    Example usage:

        def main():
            phsids = ["phs001194.v3.p2", "phs000571.v6.p2"]
            dbgapstudyreg = nih.dbgapStudyRegistration()
            metadata = dbgapstudyreg.get_metadata_for_ids(phsids=phsids)
            print(metadata)

        if __name__ == "__main__":
            main()
    """

    def __init__(
        self, api="https://dbgap.ncbi.nlm.nih.gov/ss/dbgapssws.cgi", auth_provider=None
    ):
        self._auth_provider = auth_provider
        self.api = api

    def get_metadata_for_ids(self, phsids):
        """
        Scrape dbGaP for the given studies and write TSV output to the filename
        provided.

        Args:
            phsids (List[str]): the list of study phsid/accession numbers
            output_filename (str): output file name to write to
            args (argparse.Namespace): arguments sent to command line
        """
        study_data = {}

        # Use a queue to make it appropriate to modify the list during iteration
        q = queue.Queue()
        [q.put(s) for s in phsids]

        while not q.empty():
            raw_phsid = q.get_nowait()

            # ensure version information is present
            phsid, version = dbgapStudyRegistration._get_phsid_and_version(raw_phsid)

            if not version:
                raise Exception(
                    "dbGaP Study Registration API requires a study version to get "
                    "accurate results. Please supply full study accessions that include "
                    "the version as '.vx'. It can optionally include other parts like consent. "
                    "For example: phsids=['phs001244.v2', 'phs001234.v1.p1.c1']. "
                    f"You supplied: {phsids}"
                )

            request_url = f"{self.api}?request=Study&phs={phsid}&v={version}"
            try:
                r = requests.get(request_url)
                root = ET.fromstring(r.text)
            except Exception as e:
                logging.error("Failed to parse data from NIH endpoint. {}".format(e))
                exit(1)

            studies = root.findall("Study")

            if len(studies) > 1:
                raise Exception()

            study_infos = studies[0].findall("StudyInfo")

            if len(study_infos) > 1:
                raise Exception()

            child_accessions = study_infos[0].findall("childAccession")
            study_data[raw_phsid] = [
                {
                    "phsid_version_participantset": child.text,
                    "phsid": f"phs{dbgapStudyRegistration._get_phsid_and_version(child.text)[0]}",
                    "version": f"v{dbgapStudyRegistration._get_phsid_and_version(child.text)[1]}",
                    "phsid_version": f"phs{dbgapStudyRegistration._get_phsid_and_version(child.text)[0]}.v{dbgapStudyRegistration._get_phsid_and_version(child.text)[1]}",
                }
                for child in child_accessions
            ]

        return study_data

    @staticmethod
    def _get_phsid_and_version(raw_phsid_string):
        parts = raw_phsid_string.split(".")
        has_version = False
        phsid = None
        version = None
        for part in parts:
            if part.startswith("v"):
                has_version = True
                version = part.lstrip("v")
            elif part.startswith("phs"):
                phsid = part.lstrip("phs")

        return phsid, version
