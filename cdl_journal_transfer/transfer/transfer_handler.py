"""Interface for handling journal transfers from source to target servers"""
# cdl_journal_transfer/transfer/handler.py

import asyncio, json, uuid, inspect, inflector

from pathlib import Path
from datetime import datetime

from cdl_journal_transfer import __version__

from cdl_journal_transfer.transfer.http_connection import HTTPConnection
from cdl_journal_transfer.transfer.ssh_connection import SSHConnection
from cdl_journal_transfer.progress.abstract_progress_reporter import AbstractProgressReporter
from cdl_journal_transfer.progress.null_progress_reporter import NullProgressReporter
from cdl_journal_transfer.progress.progress_update_type import ProgressUpdateType

class TransferHandler:

    STRUCTURE = {
        "journals": [
            "roles",
            "issues",
            "sections"#,
            #"articles": {}
        ]
    }

    def __init__(self, data_directory: str, source: dict = None, target: dict = None, **options):
        self.data_directory = Path(data_directory) / "current"
        self.source = source
        self.target = target
        self.options = options
        self.source_connection = self.__connection_class(source)(**self.source) if source is not None else None
        self.target_connection = self.__connection_class(target)(**self.target) if target is not None else None

        self.inflector = inflector.English()
        self.uuid = uuid.uuid1()
        self.initialize_data_directory()


    def initialize_data_directory(self) -> None:
        """Creates initial metadata file"""
        file = self.data_directory / "index.json"
        file.touch()
        now = datetime.now()

        content = {
            "application": "CDL Journal Transfer",
            "version": __version__,
            "initiated": now.strftime("%Y/%m/%d at %H:%M:%S"),
            "transaction_id": str(self.uuid)
        }

        open(file, "w").write(json.dumps(content))


    def fetch_data(self, journal_paths: list, progress_reporter: AbstractProgressReporter = NullProgressReporter(None)) -> None:
        """
        Fetches data from the source connection and writes it all to files in the data directory.

        This method accepts `journal_paths` as a filter to be included as part of fetching the
        journal index. This effectively serves as a journal filter for the entire operation.

        Process:
            - Build basic data directory structures and metadata
            - Fetch journals index
            - For each entry in the journals index, fetch an index of subresources
            - Journal by journal, pull down the journal metadata, then data for each
              individual subresource defined in their indexes. As we do this, pull down
              associated records such as users and files.

        Parameters:
            journal_paths: list
                Paths/codes of journals to be fetched
            progress_reporter: AbstractProgressReporter
                A progress reporter instance used to update the UI
        """
        self.progress_reporter = progress_reporter
        self.progress_length = 0
        self.progress_reporter.debug("Initializing...")
        self.__build_indexes(journal_paths)
        # self.__fetch_data()


    def put_data(self) -> None:
        """
        WIP
        """
        pass


    ## Private

    ## Connection handlers

    def __do_fetch(self, api_path, file, **args) -> None:
        """
        Performs a get request on the connection and commits the content to a given file.

        Parameters:
            api_path: str
                The path to direct the connection to (URL or CLI command, perhaps).
            file: Path
                The path to the file to which write the response JSON.
            args: dict
                Arbitrary kwargs to pass to the connection class.

        Returns:
            Union[list, dict]
                The JSON response
        """
        if self.source is None : return

        response = self.source_connection.get(api_path, **args)
        self.__assign_uuids(response)
        data = json.dumps(response, indent=2)

        with open(file, "w") as f:
            f.write(data)

        return response


    async def __do_put(self, record_name, data=None) -> None:
        """WIP"""
        if self.target is None : return

        if data is None:
            data_dir = self.get_data_dir(record_name)
            with open(data_dir / "index.json") as f:
                data = json.loads(f.read())

        response = await self.target_connection.put_data("journals", data)


    def __connection_class(self, server_def):
        """
        Determines the connection class to use for a server

        Parameters:
            server_def: dict
                The server definition
        """
        if server_def["type"] == "ssh":
            return SSHConnection
        elif server_def["type"] == "http":
            return HTTPConnection


    ## Sausage makers

    def __build_indexes(self, journal_paths: list) -> None:
        """
        Pulls down indexes for all requested journals and their subresources.

        Parameters:
            journal_paths: str
                List of journal paths/codes in the source server to filter on.
        """
        self.progress_reporter.debug("Fetching journal index from source.")

        subresource_count = len(self.STRUCTURE["journals"])
        journal_index_file, journal_index = self.__build_index_file(self.data_directory, "journals", paths = ",".join(journal_paths))

        self.progress_reporter.major("Fetching indexes...", len(journal_index))

        for index, journal in enumerate(journal_index):
            self.progress_reporter.minor(index, f"Fetching indexes for journal: {journal['title']}", subresource_count)

            journal_uuid = journal["uuid"]
            journal_source_pk = self.__source_pk(journal)

            journal_dir = journal_index_file.parent / journal_uuid
            journal_dir.mkdir()

            for index, subresource in enumerate(self.STRUCTURE["journals"]):
                self.progress_reporter.detail(index, f"Fetching {subresource} index")
                file, data = self.__build_index_file(journal_dir, subresource, url = f"journals/{journal_source_pk}/{subresource}")
                self.progress_reporter.debug(f"Indexed {len(data)} {subresource} record(s).")
                self.progress_reporter.debug(f"{subresource} index for journal '{journal['title']}' written to file {str(file)}.")

            self.progress_reporter.detail(subresource_count, "Done!", debug_message = f"Finished fetching indexes for {journal['title']}")


    def __build_index_file(self, base_path: Path, resource_name: str, url: str = None, **fetch_params) -> Path:
        """
        Builds an index.json file for a given path and resource.

        Parameters:
            root_path: Path
                The path (excluding the resource name) where the file should be located.
            resource_name: str
                The name of the resource the index is being created for.
            url: str
                The api path (URL or CLI command path) from which to fetch the index.
            fetch_params: dict
                Arbitrary parameters to pass to the connection handler.

        Returns: tuple(Path, Union[list, dict])
            A tuple containing the path to the newly-created index file, and its content.
        """
        pluralized_name = self.inflector.pluralize(resource_name)
        dir_path = base_path / pluralized_name
        dir_path.mkdir()
        file_path = dir_path / "index.json"
        file_path.touch()

        url = url or pluralized_name

        response = self.__do_fetch(url, file_path, **fetch_params)
        self.progress_length += len(response)
        return (file_path, response)


    def __source_pk(self, object_dict: dict) -> str:
        """
        Extracts the primary key from the "source_record_key" index entry.

        Parameters:
            object_dict: dict
                The index entry

        Returns: str
            The source primary key.
        """
        if not object_dict.get("source_record_key") : return
        return object_dict["source_record_key"].split(":")[-1]


    def __fetch_journal(self, journal: dict):
        """
        Fetches data for a provided journal stub
        """
        self.current_journal_path = self.data_directory / "journals" / journal["uuid"]
        dir = self.current_journal_path.mkdir()
        file = self.current_journal_path / "journal.json"
        file.touch()

        self.current_journal_source_id = journal["source_record_key"].split(":")[-1]
        self.__do_fetch(f"journals/{self.current_journal_source_id}", file)

        self.__fetch_roles_and_users(journal)

        for subresource in ["issues", "sections"]:
            self.__fetch_subresource(subresource, journal)


    def __fetch_subresource(self, name, journal):
        path = self.current_journal_path / name
        dir = path.mkdir()
        file = path / "index.json"
        file.touch()

        self.__do_fetch(f"journals/{self.current_journal_source_id}/{name}", file)

        with open(path / "index.json") as f:
            subresource_index = json.loads(f.read())

        for index, subresource in enumerate(subresource_index):
            sub_path = path / subresource["uuid"]
            sub_dir = sub_path.mkdir()
            sub_file = sub_path / f"{self.inflector.singularize(name)}.json"
            sub_file.touch()
            subresource_id = subresource["source_record_key"].split(":")[-1]
            self.__do_fetch(f"journals/{self.current_journal_source_id}/{name}/{subresource_id}", sub_file)


    def __fetch_roles_and_users(self, journal):
        roles_file = self.current_journal_path / "roles.json"
        roles_file.touch()

        self.__do_fetch(f"journals/{self.current_journal_source_id}/roles", roles_file)

        users_path = self.data_directory / "users"
        users_dir = users_path.mkdir(exist_ok=True)

        with open(roles_file) as f:
            roles = json.loads(f.read())

        # for user in roles:






    ## Utilities

    def __assign_uuids(self, json):
        if type(json) is list:
            for entry in json:
                self.__assign_uuids(entry)
        elif json.get("source_record_key"):
            json["uuid"] = str(uuid.uuid5(self.uuid, json["source_record_key"]))
