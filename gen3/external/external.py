class ExternalMetadataSourceInterface(object):
    """
    A simple interface for external metadata sources. The idea is to have
    consistency that allows combining the outputs and making similar function
    calls with similar input.

    test0 = dbgapFHIR()
    metadata_0 = test0.get_metadata_for_ids(["foo", "bar"])

    test1 = NIHReporter()
    metadata_1 = test1.get_metadata_for_ids(["foo", "bar"])

    test2 = dbgapDOI()
    metadata_2 = test2.get_metadata_for_ids(["foo", "bar"])

    test3 = somethingElse()
    metadata_3 = test3.get_metadata_for_ids(["foo", "bar"])

    all_metadata = metadata_0 + metadata_1 + metadata_2 + metadata_3
    """

    def __init__(
        self,
        api="",
        auth_provider=None,
    ):
        self.api = api
        self._auth_provider = auth_provider

    def get_metadata_for_ids(ids):
        """
        Returns a dictionary with the id as the key and associated metadata
        as another dictionary of values.

        Example, given: ["foo", "bar"], return:
        {
            "foo": {"name": "Foo", "description": "this is something"},
            "bar": {"name": "Bar", "description": "this is also something"},
        }

        Args:
            ids (List[str]): list of IDs to query for

        Returns:
            Dict[dict]: metadata for each of the provided IDs (which
                        are the keys in the returned dict)
        """
        raise NotImplementedError()
