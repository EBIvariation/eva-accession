import psycopg2

def get_pg_conn_for_species(species_info):
    return psycopg2.connect("dbname='dbsnp_{0}' user='{1}' host='{2}' port='{3}'".
                            format(species_info["dbsnp_build"], "dbsnp", species_info["pg_host"],
                                   species_info["pg_port"]))


def get_contiginfo_table_list_for_schema(pg_cursor, schema_name):
    pg_cursor.execute(
        """
        select string_agg(table_name,',' order by table_name) from information_schema.tables where 
        table_schema = 'dbsnp_{0}' and table_name like 'b%contiginfo';
        """.format(schema_name.lower())
    )
    results = pg_cursor.fetchall()
    if not results:
        return None
    return results[0][0].split(",")


def get_species_pg_conn_info(pg_metadata_dbname, pg_metadata_user, pg_metadata_host):
    pg_conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}'".
                               format(pg_metadata_dbname, pg_metadata_user, pg_metadata_host))
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute("select database_name,dbsnp_build,pg_host,pg_port from dbsnp_ensembl_species.import_progress a "
                      "join dbsnp_ensembl_species.dbsnp_build_instance b on b.dbsnp_build = a.ebi_pg_dbsnp_build")
    species_set = [{"database_name": result[0], "dbsnp_build":result[1], "pg_host":result[2], "pg_port":result[3]}
                   for result in pg_cursor.fetchall()]
    pg_cursor.close()
    pg_conn.close()
    return species_set