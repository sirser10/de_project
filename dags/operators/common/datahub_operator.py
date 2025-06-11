from typing import List, Union, Any
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.schema_classes import (
                                            AuditStampClass,
                                            DomainsClass,
                                            SchemaMetadataClass,
                                            SchemaFieldClass,
                                            SchemaFieldDataTypeClass,
                                            SchemalessClass,
                                            DatasetPropertiesClass,
                                            NumberTypeClass,
                                            StringTypeClass,
                                            DateTypeClass,
                                            TimeTypeClass,
                                            ArrayTypeClass,
                                            EditableDatasetPropertiesClass,
                                            SystemMetadataClass,
                                            GlobalTagsClass,
                                            GlossaryTermsClass,
                                            GlossaryTermAssociationClass,
                                            TagAssociationClass,
                                            StatusClass, 
                                            )
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
                                                                DatasetLineageType,
                                                                FineGrainedLineage,
                                                                FineGrainedLineageDownstreamType,
                                                                FineGrainedLineageUpstreamType,
                                                                Upstream,
                                                                UpstreamLineage
                                                            )
from airflow.models.baseoperator import BaseOperator
from operators.common.pg_operator import CustomPGOperator
from hooks.pg_hook import CustomPGHook

import pandas as pd
import time
import logging
import uuid
import datahub.emitter.mce_builder as builder

log = logging.getLogger(__name__)


class CustomDatahubOperator(BaseOperator):

    """
    Класс создан для загрузки Postgres сущностей и их атрибутов в DataHub.
    Так как в класс наследуется от Airflow BaseOperator, необходимо инициализировать аргумент task_id.
    """

    def __init__(
                self,
                task_id: str,
                pg_conn_id: str,
                task_key: str, 
                datahub_conn_cfg: dict,
                data_catalog_cfg: dict, 
                **kwargs
                ) -> None:
        
        super().__init__(task_id=task_id, **kwargs)

        self.domain_name   = datahub_conn_cfg['domain_name']
        self.db_name       = datahub_conn_cfg['db_name']
        self.platform_name = datahub_conn_cfg['platform_name']
        self.url           = datahub_conn_cfg['url']
        self.datahub_token = datahub_conn_cfg['datahub_token']
        self.pg_conn_id    = pg_conn_id

        self.pg_hook = CustomPGHook(env=pg_conn_id)
        self.conn    = self.pg_hook.get_conn()
        self.cursor  = self.conn.cursor()

        self.domain = DomainsClass(domains=[builder.make_domain_urn(self.domain_name)])
        self.ch_platform = builder.make_data_platform_urn(self.platform_name)
        self.graph = DataHubGraph(
                                    config=DatahubClientConfig(
                                                                server=self.url,
                                                                token=self.datahub_token
                                                                )
                                )

        self.data_catalog_cfg = data_catalog_cfg

        self.task_key = task_key
        self.exec_method = {'get_metadata_from_postgres': self.get_metadata_from_postgres}


    def execute(self, context: Any):
        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res

        
    def get_metadata_from_postgres(self) -> None:

        target_schema         = self.data_catalog_cfg['target_schema']
        target_table          = self.data_catalog_cfg['target_table']
        table_description     = self.data_catalog_cfg['table_description']
        field_description_cfg = self.data_catalog_cfg['field_description_cfg']
        column_tags           = self.data_catalog_cfg['column_tags']
        column_terms          = self.data_catalog_cfg['column_terms']


        sql_info_cfg={
                    'select_column' : [
                                          'table_schema'
                                        , 'table_name'
                                        , 'column_name'
                                        , 'is_nullable'
                                        , 'data_type'     
                                    ],
                    'info_table':'columns'
                }
        self.pg_operator = CustomPGOperator(pg_hook_con=self.pg_conn_id)
        sql_info_query: str = self.pg_operator.sql_information_schema(
                                                                        target_schema=target_schema,
                                                                        target_table=target_table,
                                                                        **sql_info_cfg
                                                                    )
        sql_info_property_params = [
                                'constraint_name', 
                                'table_catalog', 
                                'table_schema',
                                'constraint_type',
                                'constraint_column_name',
                            ]
        
        sql_info_property_query = f"""
                WITH EXCLUDE_CHECK AS (
                                SELECT 
                                    tc.constraint_name
                                    , tc.constraint_schema
                                    , tc.table_catalog
                                    , tc.table_schema
                                    , tc.table_name 
                                    , tc.constraint_type
                                FROM information_schema.table_constraints tc
                                WHERE 
                                        tc.table_schema not in ('pg_catalog') 
                                    AND tc.constraint_type not in ('CHECK')
                )
                SELECT 
                    tc.*
                    , kcu.column_name as constraint_column_name
                FROM EXCLUDE_CHECK tc
                LEFT JOIN information_schema.key_column_usage kcu USING (constraint_name, constraint_schema)
                WHERE 
                    tc.table_schema='{target_schema}'
                    AND tc.table_name='{target_table}'
                """
        try:
            self.cursor.execute(sql_info_query)
            df_cols_columns = [col[0] for col in self.cursor.description]
            df_col_values = self.cursor.fetchall()
            df_cols = pd.DataFrame(df_col_values, columns=df_cols_columns).fillna('')

            self.cursor.execute(sql_info_property_query)
            df_prprts_columns = [col[0] for col in self.cursor.description]
            df_prprts_values = self.cursor.fetchall()
            df_properties = pd.DataFrame(df_prprts_values, columns=df_prprts_columns)

            log.info(f'Starting processing {target_schema}.{target_table} for DataHub...')

            if df_cols.empty or df_properties.empty:
                raise Exception('The table metadata not received from postgres table')
            
            custom_properties = {}
            emit_aspects, fields = [], []

            for i, row in df_cols.iterrows():
                field_description = field_description_cfg.get(row['column_name'], 'No description available')
                tags = column_tags.get(row['column_name']) if column_tags else None
                terms = column_terms.get(row['column_name']) if column_terms else None
                fields.append(
                            SchemaFieldClass(
                                            fieldPath=row['column_name'],
                                            nativeDataType=row['data_type'],
                                            type=SchemaFieldDataTypeClass(self.set_ch_type_to_datahub_type(row['data_type'])),
                                            description=field_description,
                                            nullable=self.is_nullable_pg_detector(row['is_nullable']),
                                            isPartOfKey=self.is_pk_pg_detector(row['is_nullable']),
                                            globalTags=self.tag_column_getter(tags),
                                            glossaryTerms=self.glossary_term_getter(terms)
                                        )
            )
            if len(df_properties):
                custom_properties = {param: str(df_properties.iloc[0][param]) for param in sql_info_property_params}

            _dataset_urn= f"{self.domain_name}.{self.db_name}.{target_schema}.{target_table}"
            dataset_urn = builder.make_dataset_urn(
                                                platform=self.platform_name,
                                                name=_dataset_urn,
                                                env="PROD"
                                                )
            log.info(f"Dataset urn is {dataset_urn}. Platform = {self.platform_name}")
            dataset_fields = SchemaMetadataClass(
                                                schemaName=self.db_name,
                                                platform=self.ch_platform,
                                                platformSchema=SchemalessClass(),
                                                version=0,
                                                hash="",
                                                fields=fields,
                                            )
            dataset_properties = DatasetPropertiesClass(
                                                    name=f"{self.platform_name}.{self.db_name}.{target_schema}.{target_table}",
                                                    customProperties=custom_properties,
                                                    description=table_description if table_description else None,
                                                )
            editable_properties = EditableDatasetPropertiesClass(description=table_description if table_description else None)
            stautus_aspect=StatusClass(removed=False)

            emit_aspects.extend([dataset_fields, dataset_properties, editable_properties, self.domain, stautus_aspect])
            self.emit_events(
                            emitter_=self.graph,
                            entity_urn=dataset_urn,
                            aspects=emit_aspects
                        )

            log.info(f'Table {target_schema}.{target_table} successfully has been loaded to DataHub.')

        except Exception as e:
            log.info(f'Error in processing table to DataHub: {e}')
            self.cursor.close()
            self.conn.close()
            raise
    

    def set_ch_type_to_datahub_type(self, col_type: str) -> StringTypeClass:

        col_type = col_type if isinstance(col_type, str) else ValueError('Incorrected column type')
        ch_type_to_datahub_type = {
                                    'bigint': NumberTypeClass,
                                    'date': DateTypeClass,
                                    'timestamp without time zone': TimeTypeClass,
                                    'text': StringTypeClass,
                                    'character':StringTypeClass,
                                    'character varying':StringTypeClass,
                                    'integer': NumberTypeClass,
                                    'jsonb': ArrayTypeClass,
                                    'numeric': NumberTypeClass,
                                    'timestamp with time zone':TimeTypeClass,
                                    'bytea': StringTypeClass,
                                }
        class_type = StringTypeClass()
        if col_type in ch_type_to_datahub_type:
            class_type = ch_type_to_datahub_type[col_type]()
        return class_type


    def is_nullable_pg_detector(self, arg: str) -> bool:
        try:
            if arg:
                col_status = True if arg.startswith('Y') else False
                return col_status
        except ValueError as e:
            log.info(f'Error in detection nullable cols: {e}')
            raise


    def is_pk_pg_detector(self, arg: str) -> bool:
        try:
            if arg:
                col_status = True if arg.startswith('N') else False
                return col_status
        except ValueError as e:
            log.info(f'Error in detection PK cols: {e}')
            raise

    def callback_message(self, err, msg):
        if err:
            log.info(f'Datahub metadata emission error: {msg}')
    

    def emit_events(
                    self,
                    emitter_: DataHubGraph,
                    entity_urn: str,
                    aspects: list
                    ) -> None:
        
        metadata = SystemMetadataClass(lastObserved=int(time.time() * 1000), runId=str(uuid.uuid4()))
        for aspect in aspects:
            event_ = MetadataChangeProposalWrapper(
                                                    entityUrn=entity_urn,
                                                    aspect=aspect,
                                                    systemMetadata=metadata,
                                                )
            emitter_.emit(event_, self.callback_message)

    
    def tag_column_getter(self, tags: Union[List[str], str]) -> GlobalTagsClass:

        if isinstance(tags, list):
            tag_list=tags
        elif isinstance(tags, str):
            tag_list=[tags]
        else:
            tag_list= None

        tags_output=[]
        if tag_list:
            log.info(f"Tags in input: {tag_list}") 
            for t in tag_list:
                tag_to_add: str =builder.make_tag_urn(t)
                tag_association_to_add = TagAssociationClass(tag=tag_to_add)
                tags_output.append(tag_association_to_add)
            
            output_tags = GlobalTagsClass(tags=tags_output)
        else:
            output_tags = None

        return output_tags


    def glossary_term_getter(self, term: str) -> GlossaryTermsClass:
        
        term_ = term if isinstance(term, str) else None
        if term_:
            term_to_add = builder.make_term_urn(term_)
            term_association_to_add = GlossaryTermAssociationClass(urn=term_to_add)
            unknown_audit_stamp = AuditStampClass(time=0, actor="urn:li:corpuser:ingestion")
            current_terms = GlossaryTermsClass(terms=[term_association_to_add], auditStamp=unknown_audit_stamp,)
        else:
            current_terms=None

        return current_terms
    


class CustomDatahubLineage(BaseOperator):

    """
    Класс создан для реализации Lineage в DataHub - графических связей (зависимостей) между datasets (таблицами/витринами).
    Так как в класс наследуется от Airflow BaseOperator, необходимо инициализировать аргумент task_id.
    """

    def __init__(
                self,
                task_id: str,
                task_key: str, 
                datahub_conn_cfg: dict,
                table_name: str,
                up_down_streams: Union[dict,str, list],
                **kwargs
                ) -> None:
        
        super().__init__(task_id=task_id, **kwargs)

        self.domain_name   = datahub_conn_cfg['domain_name']
        self.db_name       = datahub_conn_cfg['db_name']
        self.platform_name = datahub_conn_cfg['platform_name']
        self.url           = datahub_conn_cfg['url']
        self.datahub_token = datahub_conn_cfg['datahub_token']
        
        self.table_name      = table_name
        self.up_down_streams = up_down_streams
        
        
        self.task_key = task_key
        self.exec_method = {'process_lineage': self.process_lineage}


    def execute(self, context: Any):
        exec_res = self.exec_method[self.task_key]()
        log.info(f"'{self.task_key}' method has been successfully executed in task_id '{self.task_id}'")
        return exec_res


    def process_lineage(self) -> None:

        table_name      = self.table_name
        up_down_streams = self.up_down_streams
        conn_dataset_urn = {
                            'platform':self.platform_name,
                            'domain':self.domain_name,
                            'db_name':self.db_name,
                        }
        
        main_table_dataset_urn = self.datasetUrn(schema=table_name.split('.')[0], tbl=table_name.split('.')[1], **conn_dataset_urn)
        emitter = DatahubRestEmitter(gms_server=self.url, token=self.datahub_token)

        if isinstance(up_down_streams, dict):
            upstream_dependencies, upstream_datasets  = [], []

            if all(['upstream' in up_down_streams, isinstance(up_down_streams['upstream'], dict)]):
                for tab, column in up_down_streams['upstream'].items():
                    upstream_dataset_urn: str = self.datasetUrn(schema=tab.split('.')[0], tbl=tab.split('.')[1], **conn_dataset_urn)
                    upstream_dataset=Upstream(dataset=upstream_dataset_urn, type=DatasetLineageType.TRANSFORMED)
                    upstream_datasets.append(upstream_dataset)

                    if isinstance(column,str):
                        upstream_w_columns = self.fldUrn(upstream_dataset_urn, column) 
                        upstream_dependencies.append(upstream_w_columns)
                    elif isinstance(column,list):
                        sublist_fld = [self.fldUrn(upstream_dataset_urn, c) for c in column]
                        upstream_dependencies.extend(sublist_fld)
            else:
                upstream_datasets, upstream_dependencies

            downstream_dependencies = []
            if isinstance(up_down_streams['downstream'], list):
                downstream_cols_list =[self.fldUrn(main_table_dataset_urn, c) for c in up_down_streams['downstream']]
                downstream_dependencies.extend(downstream_cols_list)

            elif isinstance(up_down_streams['downstream'], str):
                downstream_col = self.fldUrn(main_table_dataset_urn,up_down_streams['downstream'])
                downstream_dependencies.append(downstream_col)

            else:
                raise ValueError('Inappropriate input format of downstream')
            

            fineGrainedLineages = [
                                    FineGrainedLineage(
                                                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                                                        upstreams=upstream_dependencies,
                                                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                                                        downstreams=downstream_dependencies
                                    )
                                ]
            fieldLineages = UpstreamLineage(upstreams=upstream_datasets, fineGrainedLineages=fineGrainedLineages)
            lineageMcp = MetadataChangeProposalWrapper(entityUrn=main_table_dataset_urn, aspect=fieldLineages)

            emitter.emit_mcp(lineageMcp)
            log.info('The lineage consisted of entities with columns dependecy completed successfully.')

        
        elif isinstance(up_down_streams, str): #in case two-entities dependency consists of the same entity
            if '.' in up_down_streams:
                pass
            else:
                raise ValueError('Check the availability of schema("schema.table") in your input within LINEAGE_DEPENDENCIES_CFG')
            
            upstream_dataset: str = self.datasetUrn(schema=up_down_streams.split('.')[0],tbl=up_down_streams.split('.')[1],**conn_dataset_urn)
            lineage_mce = builder.make_lineage_mce([upstream_dataset],main_table_dataset_urn)
            emitter.emit_mce(lineage_mce)
            log.info('The lineage consisted of only two entities without columns dependecy completed successfully.')
        
        elif isinstance(up_down_streams, list): #in case two-entities dependency consists of UNION-entity
            upstream_lst=[]
            for entity in up_down_streams:
                upstream_dataset: str = self.datasetUrn(schema=entity.split('.')[0], tbl=entity.split('.')[1], **conn_dataset_urn)
                upstream_lst.append(upstream_dataset)

            lineage_mce = builder.make_lineage_mce(upstream_lst,main_table_dataset_urn)
            emitter.emit_mce(lineage_mce)
            log.info('The lineage dependency consisted of UNION-entity completed successfully.')

        else:
            raise ValueError('Lineage cannot be processed due to error in lineage config input. Check entity in it')


    def datasetUrn(
                self,
                platform: str,
                domain: str,
                db_name: str,
                schema: str,
                tbl: str,
                env="PROD"
                ) -> str:
        
        full_name_dataset: str = "{}.{}.{}.{}".format(domain,db_name,schema,tbl)
        return builder.make_dataset_urn(platform=platform, name=full_name_dataset, env=env)
    
    def fldUrn(self, tbl: str, fld: str)-> str:
        return builder.make_schema_field_urn(tbl, fld)