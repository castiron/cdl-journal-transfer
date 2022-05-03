"""Handler for HTTP connections to host servers. Subclass of AbstractConnection."""

import json, requests

from typing import Union, Any

from cdl_journal_transfer.transfer.abstract_connection import AbstractConnection

class HTTPConnection(AbstractConnection):

    def get(self, path: str, **params) -> Union[list, dict]:
        """
        Submits a GET request to the connection.

        Parameters:
            path: str
                The path to be appended to the server's "host" value
            params: dict
                Arbitrary parameters to be submitted as URL params

        Returns: Union[list, dict]
            The response JSON.
        """
        url = f"{self.host.strip('/')}/{path.strip('/')}"
        request_opts = self.__build_get_params(params)
        response = requests.get(url, **request_opts)
        return response


    def post(self, path: str, data) -> dict:
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
        request_opts = self.__build_post_params(data)
        response = None

        if type(data) is list:
            response = []
            for index, record in enumerate(data):
                response.append(requests.post(url, **request_opts))
        else:
            response = requests.post(url, **request_opts)

        return response


    # Private

    def __build_get_params(self, params: dict = None) -> dict:
        ret = {
            **self.__credentials(),
            "params": params
        }

        return { k:v for (k,v) in ret.items() if v is not None }


    def __build_post_params(self, params: dict = None) -> dict:
        files = params.pop("files") if "files" in params else None
        data_key = "data" if files else "json"

        ret = {
            **self.__credentials(),
            "files": files,
            data_key: params
        }

        return { k:v for (k,v) in ret.items() if v is not None }


    def __credentials(self) -> dict:
        """
        Builds credentials kwargs, if username is defined.

        Returns: dict
            The auth dict to possibly be included in the request.
        """
        if self.username is None : return {}
        return { "auth": (self.username, self.password) }
