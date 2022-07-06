"""Interface for handling journal transfers from source to target servers"""
# journal_transporter/transfer/handler.py

from pathlib import Path
from datetime import datetime

import json
import uuid
import re
import inflector

from journal_transporter import __version__

from journal_transporter.transfer.http_connection import HTTPConnection
from journal_transporter.transfer.ssh_connection import SSHConnection

from journal_transporter.progress.abstract_progress_reporter import AbstractProgressReporter
from journal_transporter.progress.null_progress_reporter import NullProgressReporter

class TransferHandler: #pylint: disable=too-many-instance-attributes
    """
    Handles the transfer of journal data from source servers to target server.

    Transfers are split into 3 distinct phases: indexing, fetching details, and pushing. For each,
    iterate through STRUCTURE, handling resources in the defined order.
    """
    STAGE_INDEXING = "indexing"
    STAGE_FETCHING = "fetching"
    STAGE_PUSHING = "pushing"
    STAGES = [STAGE_INDEXING, STAGE_FETCHING, STAGE_PUSHING]

    DEFAULT_PREPROCESSOR = "_noop_preprocessor"
    DEFAULT_INDEX_HANDLER = "_fetch_index"
    DEFAULT_FETCH_HANDLER = "_fetch_data"
    DEFAULT_PUSH_HANDLER = "_push_data"

    # STRUCTURE
    # A dict that defines the resources to be indexed, fetched, and pulled, and
    # defines any custom behaviors required for each. Resources will be handled in the order
    # defined here.
    #
    # For each resource, define a key with the name of the resource. The value must be a dict
    # containing any (or none) of the following keys:
    #   index: define behavior of the index operations
    #   fetch: define behavior of the fetch operation
    #   push: define behavior of the push operation
    #   foreign_keys: a dict of { fk_key_name: fk_resource_name }
    #   children: child definition resources (following the same resource structure)
    #
    # Operation definitions:
    #   For each operation key, the values can be either {False} to skip the operation, or a dict
    #   with any (or none) of the following keys:
    #       handler: the name of the method to handle this operation in place of the default
    STRUCTURE = {
        "users": {
            # Users are indexed as part of journals/roles in order to collect only
            # those users who have roles relevant to this transfer operation.
            "index": False,
            # "push": False
        },
        "journals": {
            "name_key": "title",
            "index": {
                "handler": "_index_journals"
            },
            "children": {
                "roles": {
                    "foreign_keys": {
                        "user": "users"
                    },
                    "index": {
                        "handler": "_index_roles"
                    },
                    "fetch": {
                        "handler": "_extract_from_index"
                    },
                    # "push": False
                },
                "issues": {
                    "name_key": "title"
                },
                "sections": {
                    "name_key": "title"
                },
                "review_forms": {
                    "children": {
                        "elements": {}
                    }
                },
                "articles": {
                    "name_key": "title",
                    "foreign_keys": {
                        "issues": "issues",
                        "sections": "sections"
                    },
                    "children": {
                        "editors": {
                            "foreign_keys": {
                                "editor": "users"
                            },
                            "fetch": {
                                "handler": "_extract_from_index"
                            }
                        },
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
                        },
                        "rounds": {
                            "children": {
                                "assignments": {
                                    "foreign_keys": {
                                        "editor": "users",
                                        "reviewer": "users",
                                        "review_file": "files",
                                        "review_form": "review_forms"
                                    },
                                    "children": {
                                        "response": {
                                            "foreign_keys": {
                                                "review_form_element": "elements"
                                            },
                                            "fetch": {
                                                "handler": "_extract_from_index"
                                            },
                                            "push": {
                                                "preprocessor": "_preprocess_assignment_responses"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    ############################
    ## INITITALIZATION / LIFECYCLE
    ############################

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
        # pylint: disable=abstract-class-instantiated
        self.source_connection = self.__connection_class(source)(**self.source) if source is not None else None
        self.target_connection = self.__connection_class(target)(**self.target) if target is not None else None

        self.inflector = inflector.English()
        self.initialize_data_directory()


    def initialize_data_directory(self) -> None:
        """Creates or parses transfer metadata file."""
        self.metadata_file = self.data_directory / "index.json"
        if self.metadata_file.exists():
            self.uuid = uuid.UUID(self.__load_file_data(self.metadata_file).get("transaction_id"))
        else:
            self.metadata_file.touch()
            now = datetime.now()
            self.uuid = uuid.uuid1()

            self.metadata = {
                "application": "Journal Transporter",
                "version": __version__,
                "transaction_id": str(self.uuid),
                "initiated": now.isoformat()
            }

            with open(self.metadata_file, "w") as file:
                file.write(json.dumps(self.metadata))


    def finalize(self) -> None:
        pass


    # Meta file management

    def write_to_meta_file(self, data: dict) -> None:
        """
        Re-writes the meta file with new data merged in.

        Parameters:
            data: dict
                The new data to write to the file. Will be appended to the end.
        """
        with open(self.metadata_file, "r") as file:
            existing_content = json.loads(file.read())
            self.metadata = { **existing_content, **data }

        self._replace_file_contents(self.metadata_file, self.metadata)


    @staticmethod
    def _replace_file_contents(file: Path, data: dict) -> None:
        """
        Replaces the contents of a file with a JSON dump.

        Useful for updating a file without messing up the formatting.
        """
        with open(file, "w") as open_file:
            open_file.seek(0)
            open_file.write(json.dumps(data, indent=2))
            open_file.truncate()


    def current_stage(self) -> str:
        """Gets the current stage the transfer is in: index, fetch, or push."""
        for stage in self.STAGES:
            started = "{0}_started".format(stage)
            finished = "{0}_finished".format(stage)
            if self.metadata.get(started) and not self.metadata.get(finished):
                return stage


    ############################
    ## PUBLIC API
    ############################

    def fetch_indexes(self, journal_paths: list) -> None:
        """
        Fetches indexes from the source connection and writes them to the data directory.

        This must be done before fetching data. Indexing will necessarily restart the
        entire process.

        Parameters:
            journal_paths: list
                Paths/codes of journals to be indexed. This will effectively filter the fetching
                process, too.
        """
        self.write_to_meta_file({ "indexing_started": datetime.now().isoformat() })
        self._index(self.STRUCTURE, journal_paths=journal_paths)
        self.write_to_meta_file({ "indexing_finished": datetime.now().isoformat() })


    def fetch_data(self, journal_paths: list) -> None:
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


    def push_data(self, journal_paths: list) -> None:
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


    ## Private-ish

    ############################
    ## CONNECTION HANDLING
    ############################

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

        Returns: Union[list, dict]
            The JSON response
        """
        if self.source is None : return

        self.progress.debug(f"GETting {api_path} with params {args}")

        try:
            response = self.source_connection.get(api_path, **args)
        except Exception as e:
            return
            return self.__handle_connection_error(e)

        if response.ok:
            self.progress.debug(f"{response}: {'File' if type == 'file' else response.text}")
            return self._handle_fetch_response(response, destination, type, order)
        else:
            return

        self.__handle_connection_error(ConnectionError(f"HTTP {response.status_code}: {response.text}"))


    def _do_push(self, api_path: str, data) -> None:
        """
        Performs a post request on the connection in order to create resources on the target server.

        Parameters:
            api_path: str
                The path to direct the connection to (URL or CLI command, perhaps).
            data: dict
                JSON-able representation of the object to be created.

        Returns: dict
            the JSON response
        """
        if self.target is None:
            return

        self.progress.debug(f"POSTing {api_path} with data {data}")

        try:
            response = self.target_connection.post(api_path, data)
        except Exception as e:
            return
            return self.__handle_connection_error(e)

        self.progress.debug(f"{response}: {response.text}")

        if response.ok:
            return response.json()
        if response.status_code < 500:
            return

        return

        self.__handle_connection_error(ConnectionError(f"HTTP {response.status_code}: {response.text}")) #pylint: disable=line-too-long


    def _handle_fetch_response(self, response, destination: Path, content_type: str, order: bool):
        """
        Handes response from fetching data, based on content type.

        For JSON data, it is written to the destination file.
        For others, it's presumed to be an attachment and is written to a file named based on
        the Content-Disposition header.

        Parameters:
            response: requests.response
                The response from the source server
            destination: Path
                Where the data should be placed.
            content_type: str<json|file>
                Determines if the content is written to a file or streamed into its own file as binary.
            order: bool
                For JSON content (specifically, lists), should it be sorted by source_record_key after being parsed?

        Returns: JSON, None
            The response content (if content_type is 'json', else None)
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
            The class to be used for the connection
        """
        if server_def.get("type") == "ssh":
            return SSHConnection

        return HTTPConnection


    ############################
    ## INDEX
    ############################

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
        for _i, (resource_name, definition) in enumerate(structure.items()):
            config = definition.get("index")

            if config == False: #pylint: disable=singleton-comparison
                continue
            if not config:
                config = {}

            self.__update_progress("Indexing", resource_name, definition, parents)

            preprocessor = self._get_preprocessor(config, self.DEFAULT_PREPROCESSOR)
            handler = self._get_handler(config, self.DEFAULT_INDEX_HANDLER)

            path = self._build_path(parents, resource_name)
            url = self._build_url(parents, resource_name)

            preprocessor(resource_name, definition, parents, path)
            response = handler(path, url, **kwargs)

            self.__increment_progress("Indexing", resource_name, definition, parents)

            for thing in response:
                if "children" in definition:
                    thing_path = path / thing["uuid"]
                    thing_path.mkdir()
                    for _j, (child_name, child_structure) in enumerate(definition["children"].items()):
                        new_parents = parents.copy()
                        new_parents[resource_name] = thing
                        self._index({ child_name: child_structure }, new_parents)


    def _fetch_index(self, path, url, **kwargs) -> list:
        """
        Default handler for fetching and writing index data.

        Parameters:
            path: Path
                The path to the file where the data should be written.
            url: str
                The URL from which index data should be fetched.
            kwargs: dict
                Arbitrary arguments to be passed to requests.

        Returns: list<dict>
            The fetched index data
        """
        path.mkdir(exist_ok=True)
        file = path / "index.json"
        file.touch()

        return self._do_fetch(url, file, order=True)


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


    def _index_roles(self, path, url, **kwargs):
        """
        Handler for fetching roles and users.

        Performs a normal indexing operation, but then also builds a users index.
        """
        roles_index = self._fetch_index(path, url, **kwargs)

        users_dir = self.data_directory / "users"
        users_dir.mkdir(exist_ok=True)
        users_index = users_dir / "index.json"

        if users_index.exists():
            existing_users_index = self.__load_file_data(users_index)
            existing_user_keys = [d.get("source_record_key") for d in existing_users_index]
        else:
            existing_user_keys = []
            users_index.touch()

        new_user_keys = [d["user"]["source_record_key"] for d in roles_index if d.get("user")]
        all_user_keys = list(set(existing_user_keys + new_user_keys))

        all_users_index_content = [{ "source_record_key": k, "uuid": self.__uuid(k) } for k in all_user_keys]
        self._replace_file_contents(users_index, all_users_index_content)

        return roles_index


    ############################
    ## FETCH
    ############################

    def _fetch(self, structure: dict, parents: dict = {}, **kwargs) -> None:
        for resource_name, definition in structure.items():
            config = definition.get("fetch")

            if config == False: #pylint: disable=singleton-comparison
                continue
            if not config:
                config = {}

            self.__update_progress("Fetching", resource_name, definition, parents)

            preprocessor = self._get_preprocessor(config, self.DEFAULT_PREPROCESSOR)
            handler = self._get_handler(config, self.DEFAULT_FETCH_HANDLER)

            if config.get("singleton"):
                path = self._build_path(parents, resource_name)
                url  = self._build_url(parents, resource_name)
                preprocessor(resource_name, definition, parents, path)
                response = handler(path, url, resource_name, None, **kwargs)
            else:
                resource_stubs = self.__load_file_data(self._build_path(parents, resource_name) / "index.json")
                for stub in resource_stubs:
                    path = self._build_path(parents, resource_name, stub)
                    url = self._build_url(parents, resource_name, stub)
                    preprocessor(resource_name, definition, parents, path, stub)
                    response = handler(path, url, resource_name, stub, **kwargs)

                    if "children" in definition:
                        for child_name, child_structure in definition["children"].items():
                            new_parents = parents.copy()
                            new_parents[resource_name] = response
                            self._fetch({ child_name: child_structure }, new_parents)


    def _fetch_data(self, path, url, resource_name, _stub, **kwargs):
        """
        Default handler for fetching detail data.

        Parameters:
            path: Path
                The path to the file where the data should be written.
            url: str
                The URL from which the detail data should be fetched.
            resource_name: str
                Name of the resource type being fetched
            _stub: dict
                noop - exists for interface parity with _extract_from_index
            kwargs: dict
                Arbitrary arguments to be passed to requests.

        Returns: dict
            The fetched data
        """
        path.mkdir(exist_ok=True)
        file = path / f"{self.inflector.singularize(resource_name)}.json"
        file.touch()

        return self._do_fetch(url, file)


    def _extract_from_index(self, path, _url, resource_name, stub, **kwargs):
        """
        Alternate fetch handler for resources with no detail endpoint.

        Parses through an index file when the file contains all necessary detail. Treats each
        entry in the index as if it were fetched from a detail view.
        """
        path.mkdir(exist_ok=True)
        file = path / f"{self.inflector.singularize(resource_name)}.json"
        file.touch()

        self._replace_file_contents(file, stub)


    def _fetch_files(self, path, url, _resource_name, stub, **kwargs):
        """
        Handler for fetching binary files (i.e. article files).

        Binary data is written without being parsed. As a result, this method does not return
        any useful data.
        """
        path.mkdir(exist_ok=True)
        file = path / "file.json"
        file.touch()
        self._replace_file_contents(file, stub)

        return self._do_fetch(url, path, "file")

    ############################
    ## PUSH
    ############################

    def _push(self, structure, parents = {}, **kwargs):
        """
        Pushes object data to a target server.

        Process:
            - Read config for preprocessor and handler
            - For each entry in an index:
                - Build path and URL for the resource
                - Run preprocessor
                - Run handler. By default, this reads the detail file for each index entry ("stub")
                  and POSTs it as JSON to the generated URL.
                - When the response comes back, take the source_record_key and write it to the
                  detail file as "target_record_key".

        Parameters:
            structure: dict<str, dict>
                The relevant segment of the structure being pushed
            parents: dict<str, dict>
                An ordered dict of the records this record is nested under
            kwargs: dict
                Arbitrary args to be passed to the handler.

        Returns: None
        """
        for _i, (resource_name, definition) in enumerate(structure.items()):
            config = definition.get("push")

            if config == False: #pylint: disable=singleton-comparison
                continue
            if not config:
                config = {}

            self.__update_progress("Pushing", resource_name, definition, parents)
            default_preprocessor = "_fetch_foreign_keys" if definition.get("foreign_keys") else self.DEFAULT_PREPROCESSOR
            preprocessor = self._get_preprocessor(config, default_preprocessor)
            handler = self._get_handler(config, self.DEFAULT_PUSH_HANDLER)

            resource_index = self.__load_file_data(self._build_path(parents, resource_name) / "index.json")

            for stub in resource_index:
                path = self._build_path(parents, resource_name, stub)
                url = self._build_url(parents, resource_name, pk_type="target")
                preprocessor(resource_name, definition, parents, path, stub)
                response = handler(path, url, resource_name, stub, **kwargs)

                if "children" in definition:
                    for child_name, child_structure in definition["children"].items():
                        new_parents = parents.copy()
                        new_parents[resource_name] = response
                        self._push({ child_name: child_structure }, new_parents)


    def _fetch_foreign_keys(self, resource_name, definition, parents, path, stub):
        """
        Default preprocessor for fetching target_record_keys for related objects.

        Searches upward within the current record's scope and writes the target_record_keys
        to the current record detail file. More complex searching requires a custom preprocessor.

        Parameters:
            resource_name: str
                The name of the current resource
            definition: dict
                The current segment of the structure
            parents: dict
                An ordered dict of parent records to which the current record belongs
            path: Path
                The path to the current resource detail file
            stub: dict
                The index stub for the current resource
        """
        fks = definition.get("foreign_keys")
        if fks:
            # Load data file for this resource
            file_dir = path / f"{self.inflector.singularize(resource_name)}.json"
            data = self.__load_file_data(file_dir)

            for fk_name, fk_resource in fks.items():
                if data.get(fk_name):
                    fk_list = data[fk_name] if isinstance(data[fk_name], list) else [data[fk_name]]
                    for fk_dict in fk_list:
                        fk_uuid = fk_dict["uuid"]
                        if fk_uuid:
                            fk_file_path = self._search_for_fk(parents, fk_resource, fk_uuid)
                            if fk_file_path.exists():
                                fk_data = self.__load_file_data(fk_file_path)
                                fk_dict["target_record_key"] = fk_data.get("target_record_key")

            self._replace_file_contents(file_dir, data)


    def _search_for_fk(self, parents: dict, fk_resource_name: str, fk_uuid: str, path: Path=None):
        """
        Recursive function to walk up the structure to find a foreign key record.

        Parameters:
            parents: dict
                Ordered dict of parent objects
            fk_resource_name: str
                The name of the resource to find
            fk__uuid: str
                The stringified resource UUID to find
            path: Path
                The path currently being searched

        Returns: Path
            The path to the file containing the foreign target_record_key
        """
        path = self.data_directory if path is None else path
        parents_clone = parents.copy()

        for subdir in path.iterdir():
            if subdir.is_dir() and subdir.name == fk_resource_name:
                return subdir / fk_uuid / f"{self.inflector.singularize(fk_resource_name)}.json"

        next_key, next_resource = list(parents_clone.items())[0]
        parents_clone.pop(next_key)
        next_path = path / next_key / next_resource["uuid"]
        return self._search_for_fk(parents_clone, fk_resource_name, fk_uuid, next_path)


    def _preprocess_assignment_responses(self, resource_name, definition, parents, path, stub):
        """
        Fetches review form element foreign keys.

        Because form elements are nested under a separate parent chain, we'll have to build
        each parent chain individually.
        """
        form = parents["assignments"].get("review_form")
        if form:
            response_detail_file = path / f"{self.inflector.singularize(resource_name)}.json"
            response_detail = self.__load_file_data(response_detail_file)
            response_element = response_detail.get("review_form_element")
            if response_element:
                element_parents = {
                    "journals": parents["journals"],
                    "review_forms": form
                }

                self._fetch_foreign_keys(resource_name, definition, element_parents, path, stub)


    def _push_data(self, path, url, resource_name, _stub, **kwargs):
        """
        Default handler for pushing an object to the target server.

        Parameters:
            path: Path
                The path to the file containing data to be pushed.
            url: str
                The URL to which the data should be pushed (POSTed).
            resource_name: str
                The name of the resource type being pushed.
            _stub: dict
                noop - exists for interface parity with alternate push methods.
            kwargs: dict
                Arbitrary arguments to be passed to requests.

        Returns: dict
            Attributes of the object created on the target server
        """
        file = path / f"{self.inflector.singularize(resource_name)}.json"
        data = self.__load_file_data(file)
        response = self._do_push(url, data)

        if response:
            data["target_record_key"] = response.get("source_record_key")
            self._replace_file_contents(file, data)
        return data


    # def _push_roles(self, _path, _url, _resource_name, stub, **kwargs):
    #     """
    #     Push both users and roles.
    #
    #     TODO: Needs to be refactored - we should be pushing users first, then roles per journal.
    #     """
    #     file = self.data_directory / "users" / stub["uuid"] / "user.json"
    #     data = self.__load_file_data(file)
    #     response = self._do_push("users", data)
    #     if response:
    #         data["target_record_key"] = response["source_record_key"]
    #         self._replace_file_contents(file, data)
    #     return data


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

            parent_file_data = self.__load_file_data(parent_file)
            metadata["parent_target_record_key"] = parent_file_data.get("target_record_key")

        files = [f for f in path.iterdir() if f.is_file() and f.name != "file.json"]
        file = files[0]
        response = self._do_push(url, { "files": { "file": open(file, "rb") }, **metadata })
        if response:
            metadata["target_record_key"] = response["source_record_key"]
            self._replace_file_contents(metadata_file, metadata)


    ############################
    ## UTILITIES
    ############################

    def _fetch_user_standalone(self, user_id):
        """
        Fetches a user from source server (if defined) and adds it to the index and detail.

        Used in case a user is referenced as a foreign key, but the user does not currently
        have a role on the journal.
        """
        path = self._build_path({}, "users")
        url = self._build_url({}, "users")
        user_detail = self._fetch_data(path, url, "users", None)

        users_index_path = self.data_directory / "users" / "index.json"
        users_index = self.__load_file_data(users_index_path)
        users_index.add({ "source_record_key": user_detail.get("source_record_key") })
        self._replace_file_contents(users_index_path, users_index)


    def _noop_preprocessor(self, *_args):
        """Noop, but avoids needing a bunch of conditionals if no preprocessing is needed."""
        pass


    def _get_handler(self, config: dict = {}, fallback_method_name = None):
        method_name = config.get("handler") or fallback_method_name
        return getattr(self, method_name) if method_name else None


    def _get_preprocessor(self, config: dict = {}, fallback_method_name = None):
        method_name = config.get("preprocessor") or fallback_method_name
        return getattr(self, method_name) if method_name else None


    def _parent_path_segments(self, parents, key):
        """
        Builds a list of strings that represents either the directory structure or URL parts
        to a given directory or resource file.

        Parameters:
            parents: dict
                A dict of parent objects, keyed by resource name
            key: str<uuid|source_pk|target_pk>
                The lookup key for the object identifier. For paths, this will typically be
                'uuid', for fetching 'source_pk', for pushing 'target_pk'.

        Returns: list
            Segments to build a path or URL
        """
        ret = []

        for (resource_name, resource_dict) in parents.items():
            ret.append(resource_name)
            if resource_dict:
                if key == "source_pk":
                    ret.append(self.__source_pk(resource_dict))
                elif key == "target_pk":
                    ret.append(self.__target_pk(resource_dict))
                elif key == "uuid":
                    ret.append(resource_dict["uuid"])

        return ret


    def _build_url(self, parents, resource_name, resource_stub=None, pk_type="source"):
        """
        Builds a URL for a given resource and its parents.

        Parameters:
            parents: dict
                A dict of parent objects, keyed by resource name
            resource_name: str
                Name of the current resource type
            resource_stub: dict
                Definition of the current resource, containing keys that can be used to build a URL
            pk_type: str<source|target>
                Whether the source or target PK should be used for nested resources

        Returns: str
            The URL to the resource
        """
        segments = self._parent_path_segments(parents, f"{pk_type}_pk") or []
        ret = f"{'/'.join(segments)}/{resource_name}"
        if resource_stub:
            if pk_type == "source":
                ret = f"{ret}/{self.__source_pk(resource_stub)}"
            elif pk_type == "target":
                ret = f"{ret}/{self.__target_pk(resource_stub)}"
        return ret



    def _build_path(self, parents, resource_name, resource_stub=None):
        """
        Builds a directory path for a given resource and its parents.

        Parameters:
            parents: dict
                A dict of parent objects, keyed by resource name
            resource_name: str
                Name of the current resource type
            resource_stub: dict, None
                Definition of the current resource, containing a UUID.

        Returns: Path
            The Path to the resource.
        """
        segments = self._parent_path_segments(parents, "uuid")
        ret = self.data_directory
        for segment in segments:
            ret = ret / segment
        ret = ret / resource_name
        if resource_stub : ret = ret / resource_stub["uuid"]
        return ret


    def __update_progress(self, action, resource_name, structure, parents):
        """
        Updates the progress reporter.

        Parameters:
            action: str
                The current operation, used as the first verb in the message
            resource_name: str
                The name of the resource currently being acted upon
            structure: dict
                The structure definition of the current resource
            parents: dict
                Resources under which the current resource is nested
        """
        message_parts = [action, resource_name]

        if len(parents) == 0:
            progress_length = len(structure.get("children")) + 1 if "children" in structure else 1
            self.minor_progress = 0
            self.progress.major(" ".join(message_parts), progress_length)
        elif len(parents) == 1:
            progress_length = self.__get_nested_child_count(structure)
            self.detail_progress = 0
            self.minor_progress = self.minor_progress + 1

            for (parent_name, parent) in parents.items():
                message_parts.extend(["for", self.inflector.singularize(parent_name), parent.get('title'), resource_name])
            message_parts = [x for x in message_parts if x]

            self.progress.minor(self.minor_progress, " ".join(message_parts), progress_length)
        else:
            self.detail_progress = self.detail_progress + 1
            self.progress.detail(self.detail_progress)


    def __increment_progress(self, action, resource_name, structure, parents):
        pass
        # if len(parents) == 0:
        #     self.minor_progress_length = self.minor_progress_length + 1
        #     new_progress = self.minor_progress_length
        # elif len(parents) > 1:
        #     self.detail_progress_length = self.detail_progress_length + 1
        #     new_progress = self.detail_progress_length
        #
        # self.progress.detail(new_progress)


    def __get_nested_child_count(self, structure):
        ret = 1
        for _, child in (structure.get("children") or {}).items():
            ret = ret + self.__get_nested_child_count(child)

        return ret


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
        # self.progress.error(error, fatal=True)
        raise error


class ConnectionError(Exception):
    pass
