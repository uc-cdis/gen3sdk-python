import requests

from gen3.utils import raise_for_status_and_print_error


class Gen3Query:
    """
    Query ElasticSearch data from a Gen3 system.

    Args:
        auth_provider (Gen3Auth): A Gen3Auth class instance.

    Examples:
        This generates the Gen3Query class pointed at the sandbox commons while
        using the credentials.json downloaded from the commons profile page.

        >>> auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        ... query = Gen3Query(auth)
    """

    def __init__(self, auth_provider):
        self._auth_provider = auth_provider

    def query(
        self,
        data_type,
        fields,
        first=None,
        offset=None,
        filters=None,
        filter_object=None,
        sort_object=None,
        accessibility=None,
        verbose=True,
    ):
        """
        Execute a query against a Data Commons.

        Args:
            data_type (str): Data type to query.
            fields (list): List of fields to return.
            first (int, optional): Number of rows to return (default: 10).
            offset (int, optional): Starting position (default: 0).
            filters: (object, optional): { field: sort method } object. Will filter data with ALL fields EQUAL to the provided respective value. If more complex filters are needed, use the `filter_object` parameter instead.
            filter_object (object, optional): Filter to apply. For syntax details, see https://github.com/uc-cdis/guppy/blob/master/doc/queries.md#filter.
            sort_object (object, optional): { field: sort method } object.
            accessibility (list, optional): One of ["accessible" (default), "unaccessible", "all"]. Only valid when querying a data type in "regular" tier access mode.

        Returns:
            Object: {"data": {<data_type>: [<record>, <record>, ...]}}

        Examples:
            >>> Gen3Query.query(
                data_type="subject",
                first=50,
                fields=[
                    "vital_status",
                    "submitter_id",
                ],
                filters={"vital_status": "Alive"},
                sort_object={"submitter_id": "asc"},
            )
        """
        if not first:
            first = 10
        if not offset:
            offset = 0
        if not sort_object:
            sort_object = {}
        if not accessibility:
            accessibility = "accessible"
        if filters and filter_object:
            raise Exception(
                "Only one of `filters` and `filter_object` can be used at a time."
            )
        if filters:
            filter_object = {
                "AND": [{"=": {field: val}} for field, val in filters.items()]
            }

        if first + offset > 10000:  # ElasticSearch limitation
            sort_fields = [{field: val} for field, val in sort_object.items()]
            data = self.raw_data_download(
                data_type=data_type,
                fields=fields,
                filter_object=filter_object,
                sort_fields=sort_fields,
                accessibility=accessibility,
                first=first,
                offset=offset,
            )
            return {"data": {data_type: data}}

        # convert sort_object to graphql: [ { field_name: "sort_method" } ]
        sorts = [f'{{{field}: "{val}"}}' for field, val in sort_object.items()]
        sort_string = f'[{", ".join(sorts)}]'

        query_string = f"""query($filter: JSON) {{
            {data_type}(
                first: {first},
                offset: {offset},
                sort: {sort_string},
                accessibility: {accessibility},
                filter: $filter
            ) {{
                {" ".join(fields)}
            }}
        }}"""
        variables = {"filter": filter_object}
        return self.graphql_query(query_string=query_string, variables=variables)

    def graphql_query(self, query_string, variables=None):
        """
        Execute a GraphQL query against a Data Commons.

        Args:
            query_txt (str): GraphQL query as text. For syntax details, see https://github.com/uc-cdis/guppy/blob/master/doc/queries.md.
            variables (:obj:`object`, optional): Dictionary of variables to pass with the query.

        Returns:
            Object: {"data": {<data_type>: [<record>, <record>, ...]}}

        Examples:
            >>> query_string = "{ my_index { my_field } }"
            ... Gen3Query.graphql_query(query_string)
        """
        url = f"{self._auth_provider.endpoint}/guppy/graphql"
        response = requests.post(
            url,
            json={"query": query_string, "variables": variables},
            auth=self._auth_provider,
        )
        try:
            raise_for_status_and_print_error(response)
        except Exception:
            print(
                f"Unable to query.\nQuery: {query_string}\nVariables: {variables}\n{response.text}"
            )
            raise
        try:
            return response.json()
        except Exception:
            print(f"Did not receive JSON: {response.text}")
            raise

    def raw_data_download(
        self,
        data_type,
        fields,
        filter_object=None,
        sort_fields=None,
        accessibility=None,
        first=None,
        offset=None,
    ):
        """
        Execute a raw data download against a Data Commons.

        Args:
            data_type (str): Data type to download from.
            fields (list): List of fields to return.
            filter_object (object, optional): Filter to apply. For syntax details, see https://github.com/uc-cdis/guppy/blob/master/doc/queries.md#filter.
            sort_fields (list, optional): List of { field: sort method } objects.
            accessibility (list, optional): One of ["accessible" (default), "unaccessible", "all"]. Only valid when downloading from a data type in "regular" tier access mode.
            first (int, optional): Number of rows to return (default: all rows).
            offset (int, optional): Starting position (default: 0).

        Returns:
            List: [<record>, <record>, ...]

        Examples:
            >>> Gen3Query.raw_data_download(
                    data_type="subject",
                    fields=[
                        "vital_status",
                        "submitter_id",
                        "project_id"
                    ],
                    filter_object={"=": {"project_id": "my_program-my_project"}},
                    sort_fields=[{"submitter_id": "asc"}],
                    accessibility="accessible"
                )
        """
        if not accessibility:
            accessibility = "accessible"
        if not offset:
            offset = 0

        body = {"type": data_type, "fields": fields, "accessibility": accessibility}
        if filter_object:
            body["filter"] = filter_object
        if sort_fields:
            body["sort"] = sort_fields

        url = f"{self._auth_provider.endpoint}/guppy/download"
        response = requests.post(
            url,
            json=body,
            auth=self._auth_provider,
        )
        try:
            raise_for_status_and_print_error(response)
        except Exception:
            print(f"Unable to download.\nBody: {body}\n{response.text}")
            raise
        try:
            data = response.json()
        except Exception:
            print(f"Did not receive JSON: {response.text}")
            raise

        if offset:
            data = data[offset:]
        if first:
            data = data[:first]

        return data
