"""
Abstract representation of a connection to a server that can export or receive journal data.
"""

import asyncio, asyncssh, json

from typing import Union, Any
from abc import ABC, abstractmethod

class AbstractConnection(ABC):

    def __init__(self, **options):
        self.host = options["host"]
        self.username = options["username"]
        self.password = options["password"]
        self.options = { k: options[k] for k in options if k not in ["host", "username", "password"] }
        self.setup()


    @abstractmethod
    def get(self, command: str, **args) -> Union[list, dict]:
        """
        Gets data from the connection.

        Parameters:
            command(str): The command or route that the connection should use.
            args(dict): Any arbitrary arguments to be passed to the connection handler.

        Returns:
            Union[list, dict]: Parsed JSON data
        """
        pass


    @abstractmethod
    def put(self, command: str, data: Any, **args) -> bool:
        """
        Puts (transfers) data to the connection.

        Parameters:
            command : str
                The command or route that the connection should use.
            data : Any
                Data to be passed to the connection. Type may vary by implementation.
            args : dict
                Any arbitrary arguments to be passed to the connection handler.

        Returns:
            bool: Was the transaction successful
        """
        pass


    def setup(self):
        """Optionally performs any necessary setup to establish or validate the connection."""
        pass
