"""Interface for handling journal transfers from source to target servers"""
# cdl_journal_transfer/transfer/handler.py

import json, uuid, inflector, re

from pathlib import Path
from datetime import datetime

from cdl_journal_transfer import __version__

from cdl_journal_transfer.transfer.http_connection import HTTPConnection
from cdl_journal_transfer.transfer.ssh_connection import SSHConnection

from cdl_journal_transfer.progress.abstract_progress_reporter import AbstractProgressReporter
from cdl_journal_transfer.progress.null_progress_reporter import NullProgressReporter
from cdl_journal_transfer.progress.progress_update_type import ProgressUpdateType

class TransferHandler:

    STAGE_INDEXING = "indexing"
    STAGE_FETCHING = "fetching"
    STAGE_PUSHING = "pushing"
    STAGES = [STAGE_INDEXING, STAGE_FETCHING, STAGE_PUSHING]

    DEFAULT_INDEX_HANDLER = "_fetch_index"
    DEFAULT_FETCH_HANDLER = "_fetch_detail"
    DEFAULT_PUSH_HANDLER = "_push_file"

    STRUCTURE = {
        "users": {
            "index": False,
            "fetch": False,
            "push": False
        },
        "journals": {
            "name_key": "title",
            "index": {
                "handler": "_index_journals"
            },
            "children": {
                "roles": {
                    "fetch": {
                        "handler": "_fetch_roles"
                    },
                    "push": {
                        "handler": "_push_roles"
                    }
                },
                "issues": {},
                "sections": {},
                "articles": {
                    "name_key": "title",
                    "foreign_keys": ["issues", "sections"],
                    "children": {
                        "authors": {
                            "fetch": {
                                "handler": "_extract_from_index"
                            }
                        },
                        "files": {
                            "fetch": {
                                "handler": "_fetch_files"
                            },
                            "push": {
                                "handler": "_push_files"
                            }
                        }
                    }
                }
            }
        }
    }

    def __init__(
        self, data_directory: str, source: dict = None, target: dict = None,
        progress_reporter: AbstractProgressReporter = NullProgressReporter(None),
        **options
    ):
        """
        Parameters:
            data_directory: str
                Directory where fetched data is stored.
            source: dict
                Connection information for the data source server.
            target: dict
                Connection information for the target server.
            progress: AbstractProgressReporter
                A progress reporter instance that can be used to update the UI.
        """
        self.data_directory = Path(data_directory) / "current"
        self.source = source
        self.target = target
        self.progress = progress_reporter
        self.options = options
        self.source_connection = self.__connection_class(source)(**self.source) if source is not None else None
        self.target_connection = self.__connection_class(target)(**self.target) if target is not None else None

        self.inflector = inflector.English()
        self.initialize_data_directory()


    def initialize_data_directory(self) -> None:
        """Creates initial metadata file"""
        self.meta_file = self.data_directory / "index.json"
        if self.meta_file.exists():
            self.uuid = uuid.UUID(self.__load_file_data(self.meta_file).get("transaction_id"))
        else:
            self.meta_file.touch()
            now = datetime.now()
            self.uuid = uuid.uuid1()

            self.meta = {
                "application": "CDL Journal Transporter",
                "version": __version__,
                "transaction_id": str(self.uuid),
                "initiated": now.isoformat()
            }

            with open(self.meta_file, "w") as file:
                file.write(json.dumps(self.meta))


    def finalize(self) -> None:
        pass


    def write_to_meta_file(self, data: dict) -> None:
        """
        Re-writes the meta file with new data merged in.

        Parameters:
            data: dict
                The new data to write to the file. Will be appended to the end.
        """
        with open(self.meta_file, "r") as file:
            existing_content = json.loads(file.read())
            self.meta = { **existing_content, **data }

        self._replace_file_contents(self.meta_file, self.meta)


    @staticmethod
    def _replace_file_contents(file, data: dict) -> None:
        """
        Replaces the contents of a file with a JSON dump.

        Useful for updating a file without messing up the formatting.
        """
        with open(file, "w") as open_file:
            open_file.seek(0)
            open_file.write(json.dumps(data, indent=2))
            open_file.truncate()


    def current_stage(self) -> str:
        for stage in self.STAGES:
            started = "{0}_started".format(stage)
            finished = "{0}_finished".format(stage)
            if self.meta.get(started) and not self.meta.get(finished):
                return stage


    ## Public API

    def fetch_indexes(self, journal_paths: list) -> None:
        """
        Fetches indexes from the source connection and writes them to the data directory.

        This must be done before fetching data. Indexing will necessarily restart the
        entire process.
        """
        self.write_to_meta_file({ "indexing_started": datetime.now().isoformat() })
        self._index(self.STRUCTURE, journal_paths=journal_paths)
        self.write_to_meta_file({ "indexing_finished": datetime.now().isoformat() })


    def fetch_data(self, journal_paths: list, progress: AbstractProgressReporter = NullProgressReporter(None)) -> None:
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
            progress: AbstractProgressReporter
                A progress reporter instance used to update the UI
        """
        self.write_to_meta_file({ "fetch_started": datetime.now().isoformat() })
        self._fetch(self.STRUCTURE)
        self.write_to_meta_file({ "fetch_finished": datetime.now().isoformat() })


    def push_data(self, journal_paths: list, progress: AbstractProgressReporter = NullProgressReporter(None)) -> None:
        """
        Pushes data from the data directory to the target connection.

        This method accepts `journal_paths` as a filter to be included as part of the push. This
        will filter the journals (by "code") that are transferred. This is mainly useful for
        when fetch and push operations are performed separately.

        Parameters:
            journal_paths: list
                Paths/codes of journals to be pushed
            progress: AbstractProgressReporter
                A progress reporter instance used to update the UI
        """
        self.write_to_meta_file({ "push_started": datetime.now().isoformat() })
        self._push(self.STRUCTURE)
        self.write_to_meta_file({ "push_finished": datetime.now().isoformat() })


    ## Private

    ## Connection handlers

    def _do_fetch(self, api_path, destination, type: str="json", order: bool=False, **args) -> None:
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

        self.progress.debug(f"GETting {api_path} with params {args}")

        try:
            response = self.source_connection.get(api_path, **args)
        except Exception as e:
            return self.__handle_connection_error(e)

        if response.ok:
            self.progress.debug(f"{response}: {'File' if type == 'file' else response.text}")
            return self._handle_fetch_response(response, destination, type, order)

        self.__handle_connection_error(ConnectionError(f"HTTP {response.status_code}: {response.text}"))


    def _do_push(self, api_path: str, data) -> None:
        """
        Performs a post request on the connection in order to create resources on the target server.

        Parameters:
            api_path: str
                The path to direct the connection to (URL or CLI command, perhaps).
            data: dict
                JSON-able representation of the object to be created.

        Returns:
            dict: the JSON response
        """
        if self.target is None : return

        self.progress.debug(f"POSTing {api_path} with data {data}")

        try:
            response = self.target_connection.post(api_path, data)
        except Exception as e:
            return self.__handle_connection_error(e)

        self.progress.debug(f"{response}: {response.text}")

        if response.ok:
            return response.json()
        elif response.status_code < 500:
            return
        else:
            self.__handle_connection_error(ConnectionError(f"HTTP {response.status_code}: {response.text}")) #pylint: disable=line-too-long


    ## Connection helpers

    def _handle_fetch_response(self, response, destination: Path, content_type: str, order: bool):
        """
        Handes response from fetching data, based on content type.

        For JSON data, it is written to the destination file.
        For others, it's presumed to be an attachment and is written to a file named based on
        the Content-Disposition header.
        """
        if content_type == "json":
            content = response.json()
            self.__assign_uuids(content)
            if order and (type(content) is list):
                content = sorted(content, key=lambda d: d["source_record_key"])

            data = json.dumps(content, indent=2)

            with open(destination, "w") as f:
                f.write(data)
                self.progress.debug(f"Written to {f.name}")

            return content
        elif content_type == "file" and response.headers.get("content-disposition", "").startswith("attachment"):
            re_result = re.search("filename=(.+)", response.headers["content-disposition"])
            filename = re_result.group(1) if re_result else "unknown_attachment"
            with open(destination / filename, "wb") as f:
                f.write(response.content)


    @staticmethod
    def __connection_class(server_def):
        """
        Determines the connection class to use for a server

        Parameters:
            server_def: dict
                The server definition

        Returns: Class<AbstractConnection>
        """
        if server_def["type"] == "ssh":
            return SSHConnection

        return HTTPConnection


    ## Utilities

    def _parent_path_segments(self, parents, key):
        ret = []

        for (resource_name, resource_dict) in parents.items():
            ret.append(resource_name)
            if key == "source_pk":
                ret.append(self.__source_pk(resource_dict))
            elif key == "target_pk":
                ret.append(self.__target_pk(resource_dict))
            elif key == "uuid":
                ret.append(resource_dict["uuid"])

        return ret


    def _build_url(self, parents, resource_name, resource_stub=None, pk_type="source"):
        segments = self._parent_path_segments(parents, f"{pk_type}_pk")
        ret = f"{'/'.join(segments)}/{resource_name}"
        if resource_stub:
            if pk_type == "source":
                ret = f"{ret}/{self.__source_pk(resource_stub)}"
            elif pk_type == "target":
                ret = f"{ret}/{self.__target_pk(resource_stub)}"
        return ret



    def _build_path(self, parents, resource_name, resource_stub=None):
        segments = self._parent_path_segments(parents, "uuid")
        ret = self.data_directory
        for segment in segments:
            ret = ret / segment
        ret = ret / resource_name
        if resource_stub : ret = ret / resource_stub["uuid"]
        return ret


    def _fetch_index(self, path, url, **kwargs) -> list:
        path.mkdir(exist_ok=True)
        file = path / "index.json"
        file.touch()

        return self._do_fetch(url, file, order=True)


    def _fetch_detail(self, path, url, resource_name, stub, **kwargs):
        path.mkdir(exist_ok=True)
        file = path / f"{self.inflector.singularize(resource_name)}.json"
        file.touch()

        return self._do_fetch(url, file)


    def _extract_from_index(self, path, url, resource_name, stub, **kwargs):
        path.mkdir(exist_ok=True)
        file = path / f"{self.inflector.singularize(resource_name)}.json"
        file.touch()

        self._replace_file_contents(file, stub)


    def _push_file(self, path, url, resource_name, stub, foreign_keys=[], **kwargs):
        file = path / f"{self.inflector.singularize(resource_name)}.json"
        data = self.__load_file_data(file)
        response = self._do_push(url, data)
        if response:
            data["target_record_key"] = response["source_record_key"]
            self._replace_file_contents(file, data)
        return data


    def __update_progress(self, action, resource_name, structure, parents, index=1):
        message = action
        for (parent_name, parent) in parents.items():
            message = message + f" {self.inflector.singularize(parent_name)} {parent.get('title')}"

        if len(parents) == 0:
            progress_length = len(structure.get("children")) + 1 if "children" in structure else 1
            self.progress.major(message, progress_length)
        elif len(parents) == 1:
            progress_length = self.__get_nested_child_count(structure)
            self.detail_progress_length = 0
            self.progress.minor(index, message, progress_length)
        else:
            self.detail_progress_length = self.detail_progress_length + 1
            self.progress.detail(self.detail_progress_length, message)


    def __get_nested_child_count(self, structure):
        ret = 1
        if type(structure) is str : breakpoint()
        for _, child in (structure.get("children") or {}).items():
            ret = ret + self.__get_nested_child_count(child)
        return ret


    ## Indexing

    def _index(self, structure: dict, parents: dict = {}, **kwargs):
        """
        Gets index files for all resources.

        As part of this process, UUIDs are assigned to all items in the indexes, and directories
        are created for each resource with children.

        This method is called recursively for each tree of children in STRUCTURE.

        Parameters:
            structure: dict
                The portion of the STRUCTURE dict currntly being indexed.
            parents: dict
                Indexed resources to which the current structure belongs.
            kwargs: dict
                Arbitrary arguments, typically used for custom handlers.
        """
        for i, (resource_name, definition) in enumerate(structure.items()):
            config = definition.get("index")

            if config == False:
                continue
            if not config:
                config = {}

            self.__update_progress(f"Indexing", resource_name, definition, parents)

            handler_name = config.get("handler") or self.DEFAULT_INDEX_HANDLER
            handler = getattr(self, handler_name)

            path = self._build_path(parents, resource_name)
            url = self._build_url(parents, resource_name)
            response = handler(path, url, **kwargs)

            for thing in response:
                if "children" in definition:
                    thing_path = path / thing["uuid"]
                    thing_path.mkdir()
                    for _j, (child_name, child_structure) in enumerate(definition["children"].items()):
                        new_parents = parents.copy()
                        new_parents[resource_name] = thing
                        self._index({ child_name: child_structure }, new_parents)


    def _index_journals(self, path, url, **kwargs) -> list:
        """
        Gets journals index.

        Same as _fetch_index, but passes paths argument to apply journal filter.
        """
        path_str = ",".join(kwargs["journal_paths"])

        path.mkdir(exist_ok=True)
        file = path / "index.json"
        file.touch()

        return self._do_fetch(url, file, order=True, paths=path_str)


    ## Fetch

    def _fetch(self, structure: dict, parents: dict = {}, **kwargs) -> None:
        for i, (resource_name, definition) in enumerate(structure.items()):
            config = definition.get("fetch")

            if config == False:
                continue
            if not config:
                config = {}

            handler_name = config.get("handler") or self.DEFAULT_FETCH_HANDLER
            handler = getattr(self, handler_name)

            self.__update_progress(f"Fetching", resource_name, definition, parents)

            resource_stubs = self.__load_file_data(self._build_path(parents, resource_name) / "index.json")
            for stub in resource_stubs:
                path = self._build_path(parents, resource_name, stub)
                url = self._build_url(parents, resource_name, stub)
                response = handler(path, url, resource_name, stub, **kwargs)

                if "children" in definition:
                    for j, (child_name, child_structure) in enumerate(definition["children"].items()):
                        new_parents = parents.copy()
                        new_parents[resource_name] = response
                        self._fetch({ child_name: child_structure }, new_parents)


    def _fetch_roles(self, path, _url, _resource_name, stub, **kwargs):
        path.mkdir(exist_ok=True)
        file = path / "role.json"
        file.touch()
        self._replace_file_contents(file, stub)

        # Users are stored in their own directory outside of the journal to prevent
        # duplication.
        users_dir = self.data_directory / "users"
        users_dir.mkdir(exist_ok=True)

        user_dir = users_dir / stub["uuid"]
        if user_dir.exists() : return

        user_dir.mkdir()
        user_file = user_dir / "user.json"
        user_file.touch()

        user_pk = self.__source_pk(stub)
        self._do_fetch(f"users/{user_pk}", user_file)


    def _fetch_files(self, path, url, resource_name, stub, **kwargs):
        path.mkdir(exist_ok=True)
        file = path / "file.json"
        file.touch()
        self._replace_file_contents(file, stub)

        return self._do_fetch(url, path, "file")


    ## Push

    def _push(self, structure, parents = {}, **kwargs):
        for _i, (resource_name, definition) in enumerate(structure.items()):
            config = definition.get("push")

            if config == False:
                continue
            if not config:
                config = {}

            self.__update_progress(f"Pushing", resource_name, definition, parents)

            handler_name = config.get("handler") or self.DEFAULT_PUSH_HANDLER
            handler = getattr(self, handler_name)

            resource_index = self.__load_file_data(self._build_path(parents, resource_name) / "index.json")

            for stub in resource_index:
                preprocessor = config.get("preprocessor")
                if preprocessor : getattr(self, preprocessor)(resource_index, stub)

                path = self._build_path(parents, resource_name, stub)
                url = self._build_url(parents, resource_name, pk_type="target")

                fks = definition.get("foreign_keys")
                if fks:
                    file_dir = path / f"{self.inflector.singularize(resource_name)}.json"
                    data = self.__load_file_data(file_dir)
                    for fk in fks:
                        for fk_item in data[fk]:
                            fk_uuid = fk_item["uuid"]
                            fk_file_path = path.parents[1] / fk / fk_uuid / f"{self.inflector.singularize(fk)}.json"
                            fk_data = self.__load_file_data(fk_file_path)
                            fk_item["target_record_key"] = fk_data["target_record_key"]

                    self._replace_file_contents(file_dir, data)

                response = handler(path, url, resource_name, stub, **kwargs)

                postprocessor = config.get("postprocessor")
                if postprocessor : getattr(self, postprocessor)(resource_index, )

                if "children" in definition:
                    for _i2, (child_name, child_structure) in enumerate(definition["children"].items()):
                        new_parents = parents.copy()
                        new_parents[resource_name] = response
                        self._push({ child_name: child_structure }, new_parents)


    def _push_roles(self, _path, _url, _resource_name, stub, **kwargs):
        file = self.data_directory / "users" / stub["uuid"] / "user.json"
        data = self.__load_file_data(file)
        response = self._do_push("users", data)
        if response:
            data["target_record_key"] = response["source_record_key"]
            self._replace_file_contents(file, data)
        return data


    def _push_files(self, path, url, _resource_name, stub, **kwargs):
        """
        Files need to be combined with their metadata from the index, then pushed as
        multipart requests.
        """
        metadata_file = path / "file.json"
        metadata = self.__load_file_data(metadata_file)

        parent_key = metadata.get("parent_source_record_key")
        if parent_key:
            parent_uuid = self.__uuid(parent_key)
            parent_file = path.parents[0] / parent_uuid / "file.json"
            if not parent_file.exists():
                breakpoint()
            parent_file_data = self.__load_file_data(parent_file)
            metadata["parent_target_record_key"] = parent_file_data.get("target_record_key")

        files = [f for f in path.iterdir() if f.is_file() and f.name != "file.json"]
        file = files[0]
        response = self._do_push(url, { "files": { "file": open(file, "rb") }, **metadata })
        if response:
            metadata["target_record_key"] = response["source_record_key"]
            self._replace_file_contents(metadata_file, metadata)


    ## Utilities

    @staticmethod
    def __source_pk(object_dict: dict) -> str:
        """
        Extracts the primary key from the "source_record_key" index entry.

        Parameters:
            object_dict: dict
                The index entry

        Returns: str
            The source primary key.
        """
        if object_dict.get("source_record_key"):
            return object_dict["source_record_key"].split(":")[-1]

        return None


    @staticmethod
    def __target_pk(object_dict: dict) -> str:
        """
        Extracts the primary key from the "source_record_key" index entry.

        Parameters:
            object_dict: dict
                The index entry

        Returns: str
            The source primary key.
        """
        if object_dict.get("target_record_key"):
            return object_dict["target_record_key"].split(":")[-1]

        return None


    def __calculate_progress_length(self, resource) -> int:
        pass


    @staticmethod
    def __load_file_data(path):
        with open(path) as file:
            return json.loads(file.read())

        return None


    def __assign_uuids(self, data):
        if type(data) is list:
            for entry in data:
                self.__assign_uuids(entry)
        elif type(data) is dict:
            if data.get("source_record_key"):
                data["uuid"] = self.__uuid(data["source_record_key"])
            for (_key, value) in data.items():
                self.__assign_uuids(value)


    def __uuid(self, key):
        return str(uuid.uuid5(self.uuid, key))


    def __structure_depth(self, structure: dict) -> int:
        ret = 0

        for key, value in structure.items():
            if value.get("children"):
                ret = ret + 1 + self.__structure_depth(value.get("children"))
            else:
                ret = ret + 1

        return ret

    ## Error Handling

    def __handle_connection_error(self, error: Exception):
        self.progress.error(error, fatal=True)
        raise error


class ConnectionError(Exception):
    pass
