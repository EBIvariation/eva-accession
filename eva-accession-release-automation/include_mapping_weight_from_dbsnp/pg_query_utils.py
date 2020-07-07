# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import psycopg2

logger = logging.getLogger(__name__)


def get_all_results_for_query(pg_conn, query):
    with get_result_cursor(pg_conn, query) as pg_cursor:
        results = pg_cursor.fetchall()
    return results


def execute_query(pg_conn, query):
    with get_result_cursor(pg_conn, query) as pg_cursor:
        pg_conn.commit()


def get_result_cursor(pg_conn, query):
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(query)
    return pg_cursor


def get_pg_connection_handle(dbname, user, host):
    return psycopg2.connect("dbname='{0}' user='{1}' host='{2}'".format(dbname, user, host))


def index_already_exists_on_table(pg_conn, schema_name, table_name, index_columns):
    index_columns_lower_case = list(map(str.lower, index_columns))
    query = """select unnest(column_names) from (
                    select
                        nmsp.nspname as schema_name,
                        t.relname as table_name,
                        i.relname as index_name,
                        array_agg(a.attname) as column_names,
                        count(*) as number_of_columns
                    from
                        pg_class t,
                        pg_class i,
                        pg_index ix,
                        pg_attribute a,
                        pg_namespace nmsp
                    where
                        t.oid = ix.indrelid
                        and i.oid = ix.indexrelid
                        and a.attrelid = t.oid
                        and a.attnum = ANY(ix.indkey)
                        and t.relkind = 'r'
                        and nmsp.oid = t.relnamespace
                        and nmsp.nspname = '{0}'
                        and t.relname = '{1}'
                        and a.attname in ({2})                        
                    group by    
                        schema_name, table_name, index_name
                    order by
                        t.relname,
                        i.relname
                    ) temp 
        where number_of_columns = {3};
    """.format(schema_name, table_name,
               ",".join(["'{0}'".format(col) for col in index_columns_lower_case]), len(index_columns))
    results = [result[0] for result in get_all_results_for_query(pg_conn, query)]
    return sorted(results) == index_columns_lower_case


def create_index_on_table(pg_conn, schema_name, table_name, index_columns):
    if index_already_exists_on_table(pg_conn, schema_name, table_name, index_columns):
        logger.info("Index on {0} column(s) on {1}.{2} already exists. Skipping..."
                    .format(",".join(list(map(str.lower, sorted(index_columns)))), schema_name, table_name))
    else:
        query = "create index on {0}.{1} ({2})".format(schema_name, table_name,
                                                       ",".join(list(map(str.lower, sorted(index_columns))))
                                                       )
        logger.info("Building index with query: " + query)
        execute_query(pg_conn, query)
        pg_conn.commit()


def vacuum_analyze_table(pg_conn, schema_name, table_name, columns=[]):
    query = "vacuum analyze {0}.{1}".format(schema_name, table_name)
    if columns:
        query += "({0})".format(",".join(columns))
    isolation_level_pre_analyze = pg_conn.isolation_level
    try:
        # This is needed for vacuum analyze to work since it can't work inside transactions!
        pg_conn.set_isolation_level(0)
        logger.info("Vacuum analyze with query: " + query)
        execute_query(pg_conn, query)
    except Exception as ex:
        logger.error(ex)
    finally:
        pg_conn.set_isolation_level(isolation_level_pre_analyze)
