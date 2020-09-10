# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import logging
import re
from typing import Union, Dict, List, Any, Optional

from amundsen_common.models.dashboard import DashboardSummary
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import Table, Column, Source, ProgrammaticDescription
from amundsen_common.models.user import User as UserEntity, User
from google.api_core.client_options import ClientOptions
from google.cloud import datacatalog_v1
from google.cloud.datacatalog_v1.proto.datacatalog_pb2 import Entry

from metadata_service.entity.dashboard_detail import DashboardDetail as DashboardDetailEntity
from metadata_service.entity.description import Description
from metadata_service.entity.resource_type import ResourceType
from metadata_service.proxy import BaseProxy
from metadata_service.util import UserResourceRel

LOGGER = logging.getLogger(__name__)


# @todo - consider moving base class with init to commons - it's exactly the same in metadatalibrary
class GCPDataCatalogProxy(BaseProxy):
    def __init__(self, *,
                 project_id: str = '',
                 credentials_file: Optional[str] = None,
                 client: datacatalog_v1.DataCatalogClient = None,
                 page_size: int = 10,
                 **kwargs
                 ) -> None:

        self.scope = datacatalog_v1.types.SearchCatalogRequest.Scope()
        self.scope.include_project_ids.append(project_id)

        client_options = ClientOptions(scopes=[self.scope])

        if credentials_file:
            _client = datacatalog_v1.DataCatalogClient.from_service_account_file(credentials_file,
                                                                                 client_options=client_options)
        else:
            _client = datacatalog_v1.DataCatalogClient(credentials=credentials_file, client_options=client_options)

        self.client = client or _client
        self.page_size = page_size

    # @todo move to commons
    @staticmethod
    def _extract_info_from_uri(table_uri: str) \
            -> Dict:
        """
        Extracts the table information from table_uri coming from frontend.
        :param table_uri:
        :return: Dictionary object, containing following information:
        entity: Type of entity example: rdbms_table, hive_table etc.
        cluster: Cluster information
        db: Database Name
        name: Table Name
        """
        pattern = re.compile(r"""
            ^   (?P<entity>.*?)
            :\/\/
                (?P<cluster>.*)
            \.
                (?P<db>.*?)
            \/
                (?P<name>.*?)
            $
        """, re.X)
        result = pattern.match(table_uri)
        return result.groupdict() if result else dict()

    def _get_resource_owners(self, resource_link) \
            -> List[User]:
        result = []

        tags = self.client.list_tags(resource_link)

        _emails = {}
        for tag in tags:
            if tag.template_display_name == 'Resource Owners':
                for k, spec in dict(tag.fields).items():
                    # key of label is owner_n where n is number from 1 to N. The convention is to have 1 for main owner,
                    # while next entries order doesn't matter. we add owners in order that positions main owner as a
                    # first one, which is not guaranteed by data catalog response structure
                    _emails[k.split('_')[1]] = spec.string_value

        for _, email in sorted(_emails.items()):
            result.append(User(user_id=email, email=email))

        return result

    # @todo make part of class stored in commons
    def _get_resource_metadata(self, resource_link, display_name_regex: str = r'.*\- Metadata$') \
            -> dict:
        result = {}

        display_name_pattern = re.compile(display_name_regex)

        tags = self.client.list_tags(resource_link)

        _entries = {}
        for tag in tags:
            # every rdbms connector creates tag template with '- Metadata' prefix containing additional metadata
            if display_name_pattern.match(tag.template_display_name):
                for k, spec in dict(tag.fields).items():
                    result[k] = spec.string_value

        return result

    def get_users(self) -> List[UserEntity]:
        pass

    def _get_table_entry(self, table_uri: str) -> (Entry, bool):
        entity_parameters = GCPDataCatalogProxy._extract_info_from_uri(table_uri)

        entity = entity_parameters['entity']

        _cluster = entity_parameters['cluster']
        project_id, project_location = _cluster.split('__')

        db_name = entity_parameters['db']
        table_name = entity_parameters['name']

        # If entity is BigQuery (which system integrated with GCP) then it's difficult to construct entry_path because
        # entry_name is a hashed id. That's why linked_resource field with lookup_entry method are used.
        # In case of other RDMBS systems ingested through available connectors, it's possible to render entry_path
        # and at the same time linked_resource property is not sufficient to pinpoint exact entry.
        if entity == 'bigquery':
            url = f'//{entity}.googleapis.com/projects/{project_id}/datasets/{db_name}/tables/{table_name}'
            entry = self.client.lookup_entry(linked_resource=url)
        else:
            # separator is a workaround since for some reason hive entry has two spaces between db and table names
            separator = '__' if entity == 'hive' else '_'
            url = self.client.entry_path(project_id, project_location, entity, f'{db_name}{separator}{table_name}')
            entry = self.client.get_entry(url)

        entity_parameters['url'] = url

        return entry, entity_parameters

    def get_table(self, *, table_uri: str) \
            -> Table:

        def is_view() -> (bool, Optional[Source]):
            if entity == 'bigquery':
                if entry.bigquery_table_spec.table_source_type == 2:
                    source_pattern = re.compile(r'FROM\s`([A-Za-z0-9\-\_\.]+)`')

                    view_query = entry.bigquery_table_spec.view_spec.view_query
                    try:
                        # assumes that view is in the same project
                        view_source = source_pattern.findall(view_query)[0].split('.')[-1]
                        _source = Source(source_type=entity.title(), source=view_source)
                    except IndexError:
                        _source = None

                    return True, _source
            # currently entries ingested through connectors do not distinguish views from tables
            return False, None

        entry, entity_parameters = self._get_table_entry(table_uri)

        entity = entity_parameters['entity']
        table_name = entity_parameters['name']
        project_id = entity_parameters['cluster']
        db_name = entity_parameters['db']
        url = entity_parameters['url']

        columns = []

        for c in entry.schema.columns:
            column = Column(name=c.column,
                            description=c.description,
                            col_type=c.type,
                            sort_order=0)

            columns.append(column)

        # @todo fill those fields
        #     tags: List[Tag] = []
        #     badges: Optional[List[Tag]] = []
        #     table_readers: List[Reader] = []
        #     watermarks: List[Watermark] = []
        #     table_writer: Optional[Application] = None
        #     resource_reports: Optional[List[ResourceReport]] = None

        is_view, source = is_view()

        resource_link = entry.name if entity == 'bigquery' else url

        programmatic_descriptions = []
        for k, v in sorted(self._get_resource_metadata(resource_link).items()):
            programmatic_descriptions.append(ProgrammaticDescription(source=k.replace('_', ' '),
                                                                     text=v.replace('_', ' ')))

        return Table(name=table_name,
                     cluster=project_id,
                     database=entity,
                     schema=db_name,
                     description=entry.description,
                     last_updated_timestamp=entry.source_system_timestamps.update_time.seconds,
                     tags=[],
                     badges=[],
                     columns=columns,
                     is_view=is_view,
                     source=source,
                     owners=self._get_resource_owners(resource_link),
                     programmatic_descriptions=programmatic_descriptions)

    def delete_owner(self, *, table_uri: str, owner: str) -> None:
        pass

    def add_owner(self, *, table_uri: str, owner: str) -> None:
        pass

    def get_table_description(self, *, table_uri: str) -> Union[str, None]:
        entry, _ = self._get_table_entry(table_uri)

        return entry.description

    def put_table_description(self, *, table_uri: str, description: str) -> None:
        entry, _ = self._get_table_entry(table_uri)

        entry.description = description

        return self.client.update_entry(entry)

    def add_tag(self, *, id: str, tag: str, tag_type: str, resource_type: ResourceType) -> None:
        pass

    def delete_tag(self, *, id: str, tag: str, tag_type: str, resource_type: ResourceType) -> None:
        pass

    def put_column_description(self, *, table_uri: str, column_name: str, description: str) -> None:
        pass

    def get_column_description(self, *, table_uri: str, column_name: str) -> Union[str, None]:
        pass

    @staticmethod
    def _process_table_entry(entry: str) -> Table:
        # @todo this bit is the same as process table resource in metadata so could be part of commons
        linked_resource_parts = entry.linked_resource.split('/')
        relative_resource_name_parts = entry.relative_resource_name.split('/')

        name = linked_resource_parts[-1]

        _database = entry.user_specified_system or entry.integrated_system

        if isinstance(_database, int):
            if _database == 1:
                database = 'bigquery'
            else:
                raise NotImplementedError(f'Integrated system {_database} is not supported')

            schema = linked_resource_parts[-3]
        else:
            database = _database
            schema = relative_resource_name_parts[-1].replace(name, '').strip('_')

        cluster = relative_resource_name_parts[1] + '__' + relative_resource_name_parts[3]

        return dict(database=database,
                    cluster=cluster,
                    schema=schema,
                    name=name)

    def _basic_search(self, entry_type: Optional[str] = None, num_entries: int = 5, order_by: Optional[str] = None):
        entries = []

        if entry_type:
            query = f'type={entry_type}'
        else:
            query = '*'

        i = 1
        for element in self.client.search_catalog(query=query, scope=self.scope, order_by=order_by):
            if i <= num_entries:
                if entry_type == 'table':
                    result = GCPDataCatalogProxy._process_table_entry(element)
                else:
                    result = element

                entries.append(result)
            else:
                break

            i += 1

        return i, entries

    def get_popular_tables(self, *, num_entries: int) -> List[PopularTable]:
        popular_tables = []

        total_results, results = self._basic_search('table', num_entries)

        for result in results:
            popular_table = PopularTable(**result)

            popular_tables.append(popular_table)

        return popular_tables

    def get_latest_updated_ts(self) -> int:
        _, search_results = self._basic_search(None, 1, 'last_modified_timestamp desc')

        for entry in search_results:
            try:
                return self.client.get_entry(entry.relative_resource_name).source_system_timestamps.update_time.seconds
            except:
                return 0

    def get_tags(self) -> List:
        pass

    def get_dashboard_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) -> Dict[
        str, List[DashboardSummary]]:
        pass

    def get_table_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) -> Dict[str, Any]:
        pass

    def get_frequently_used_tables(self, *, user_email: str) -> Dict[str, Any]:
        pass

    def add_resource_relation_by_user(self, *, id: str, user_id: str, relation_type: UserResourceRel,
                                      resource_type: ResourceType) -> None:
        pass

    def delete_resource_relation_by_user(self, *, id: str, user_id: str, relation_type: UserResourceRel,
                                         resource_type: ResourceType) -> None:
        pass

    def get_dashboard(self, id: str) -> DashboardDetailEntity:
        entry = self.client.get_entry(id)
        dashboard_metadata = self._get_resource_metadata(entry.name, r'.*Dashboard Metadata$')
        # Workbook Metadata could be used for tables
        workbook_metadata = self._get_resource_metadata(entry.name, r'.*Workbook Metadata$')
        relative_resource_name_parts = entry.name.split('/')

        # common resource properties
        _name = dashboard_metadata.get('workbook_name')
        _description = entry.description
        _owners = self._get_resource_owners(entry.name)
        _frequent_users = []  # @todo dashboard - frequent users (can be also used for table)

        # dashboard specific properties
        _uri = entry.name
        _cluster = relative_resource_name_parts[1] + '__' + relative_resource_name_parts[3]
        _group_name = dashboard_metadata.get('site_name')
        _group_url = ''
        _product = entry.user_specified_system or entry.integrated_system
        _url = ''  # @todo dashboard - url
        _created_timestamp = entry.source_system_timestamps.create_time.seconds
        _updated_timestamp = entry.source_system_timestamps.update_time.seconds
        _last_successful_run_timestamp = 0  # @todo dashboard - last successful run timestamp
        _last_run_timestamp = 0  # @todo dashboard - last run timestamp
        _last_run_state = dashboard_metadata.get('last_run_state', 'Unknown')  # @todo dashboard - last run state
        _recent_view_count = dashboard_metadata.get('recent_view_count', 0)  # @todo dashboard - recent view count
        _chart_names = []  # @todo - dashboard chart names
        _queries = []  # @todo - dashboard queries
        _tables = [self._get_table_details_from_dashboard_metadata(t, _cluster)
                   for t in workbook_metadata.get('upstream_tables', '').split(',')]  # @todo - dashboard tables
        data = dict(
            uri=_uri, cluster=_cluster, group_name=_group_name, group_url=_group_url, product=_product, name=_name,
            url=_url, description=_description, created_timestamp=_created_timestamp,
            updated_timestamp=_updated_timestamp, last_successful_run_timestamp=_last_successful_run_timestamp,
            last_run_timestamp=_last_run_timestamp, last_run_state=_last_run_state,
            recent_view_count=_recent_view_count, owners=_owners, frequent_users=_frequent_users, tables=_tables
        )

        result = DashboardDetailEntity(**data)
        """
        frequent_users: List[User] = attr.ib(factory=list)
        chart_names: List[str] = attr.ib(factory=list)
        queries: List[DashboardQuery] = attr.ib(factory=list)
        tables: List[PopularTable] = attr.ib(factory=list)
        tags: List[Tag] = attr.ib(factory=list)
        badges: List[Tag] = attr.ib(factory=list)
        """

        return result

    def _get_table_details_from_dashboard_metadata(self, table_details: str, cluster: str) -> dict:
        pattern = re.compile(r"""
                    ^   (?P<database>[A-Za-z0-9\s]+)
                    
                        (\((?P<conn>.*)\))
                    \/
                        (\[(?P<schema>[A-Za-z\s]+)\])
                    \.
                        (\[(?P<name>[A-Za-z\s]+)\])
                    $
                """, re.X)

        _result = pattern.match(table_details)

        result = _result.groupdict() if _result else dict()

        if result:
            result['cluster'] = cluster

        return result

    def get_dashboard_description(self, *, id: str) -> Description:
        pass

    def put_dashboard_description(self, *, id: str, description: str) -> None:
        pass

    def get_resources_using_table(self, *, id: str, resource_type: ResourceType) -> Dict[str, List[DashboardSummary]]:
        pass

    def get_user(self, *, id: str) -> Union[UserEntity, None]:
        pass
