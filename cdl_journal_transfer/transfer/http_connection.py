"""Handler for HTTP connections to host servers. Subclass of AbstractConnection."""

import json, requests

from typing import Union, Any

from cdl_journal_transfer.transfer.abstract_connection import AbstractConnection

class HTTPConnection(AbstractConnection):

    def get(self, path: str, **args) -> Union[list, dict]:
        """
        Submits a GET request to the connection.

        Parameters:
            path: str
                The path to be appended to the server's "host" value
            args: dict
                Arbitrary parameters to be submitted as URL params

        Returns: Union[list, dict]
            The response JSON.
        """
        url = f"{self.host.strip('/')}/{path.strip('/')}"
        request_opts = {**self.__credentials(), **{"params": args}}
        response = requests.get(url, **request_opts)
        return json.loads(response.text or "[]")


    def put(self, path: str, data) -> bool:
        """
        Submits a POST request to the connection.

        Parameters:
            path: str
                The path to be appended to the server's "host" value
            data: Any
                Any serializable content to be submitted as POST data.

        Returns: Any
            The response content.
        """
        url = f"{self.host.strip('/')}/{path.strip('/')}/"
        request_opts = self.__credentials()
        if type(data) is list:
            for index, record in enumerate(data):
                response = requests.post(url, json=record, **request_opts)
        else:
            requests.post(url, json=data, **request_opts)


    # Private

    def __credentials(self) -> dict:
        """
        Builds credentials kwargs, if username is defined.

        Returns: dict
            The auth dict to possibly be included in the request.
        """
        if self.username is None : return {}
        return { "auth": (self.username, self.password) }
