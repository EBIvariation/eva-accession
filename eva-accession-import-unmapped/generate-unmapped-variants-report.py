# Copyright 2019 EMBL - European Bioinformatics Institute
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

from __init__ import *
import os
import sys
import argparse
import datetime
import textwrap
# We need this because static relative imports cannot be done when there are hyphens in the folder name!
sys.path.append("../eva-accession-import-automation")
import data_ops


def get_contigloc_table_list_for_schema(species_info):
    species_pg_conn = data_ops.get_pg_conn_for_species(species_info)
    species_pg_cursor = species_pg_conn.cursor()
    species_pg_cursor.execute(
        """
        select string_agg(table_name,',' order by table_name) from information_schema.tables where 
        table_schema = 'dbsnp_{0}' and table_name ~ 'b\d+_snpcontigloc$';
        """.format(species_info["database_name"].lower())
    )
    results = species_pg_cursor.fetchall()
    species_pg_cursor.close()
    species_pg_conn.close()
    if results[0][0] is None:
        return []
    return results[0][0].split(",")


def does_subsnpseq_table_exist_for_species(species_info):
    species_pg_conn = data_ops.get_pg_conn_for_species(species_info)
    species_pg_cursor = species_pg_conn.cursor()
    species_pg_cursor.execute("select table_name from information_schema.tables where "
                              "table_schema = 'dbsnp_{0}' and lower(table_name) in ('subsnpseq3', 'subsnpseq5');"
                              .format(species_info["database_name"]))
    subsnpseq_tables = [result[0] for result in species_pg_cursor.fetchall()]
    species_pg_cursor.close()
    species_pg_conn.close()
    return len(subsnpseq_tables) == 2


def get_report_creation_query(species_info):
    work_mem_clause = "set work_mem to '128MB';"

    temp_table_creation_query = "create temp table snp_ids (snp_id bigint);" + os.linesep
    temp_table_creation_query += os.linesep.join(["insert into snp_ids select snp_id from dbsnp_{0}.{1};"
                                                 .format(species_info["database_name"], table)
                                                  for table in species_info["contigloc_table_list"]]) + os.linesep
    temp_table_creation_query += "create index on snp_ids (snp_id); analyze snp_ids;"
    species_info["temp_table_creation_query"] = temp_table_creation_query

    subsnpseq_index_query = ("create index on dbsnp_{database_name}.subsnpseq3(subsnp_id); " +
                             "create index on dbsnp_{database_name}.subsnpseq5(subsnp_id);").format(**species_info)

    contigloc_snp_id_query = \
        """
        create table if not exists dbsnp_{database_name}.contigloc_snp_ids_all_builds as 
            (select snp_id from snp_ids group by 1);
        create index if not exists contigloc_snp_ids_idx on 
            dbsnp_{database_name}.contigloc_snp_ids_all_builds using btree(snp_id); 
         """.format(**species_info)
    unmapped_variants_report_query = \
        """
        create table if not exists dbsnp_{database_name}.unmapped_variants_all_builds as (
            select 
                '{database_name}' as database_name,
                lnk.subsnp_id, lnk.snp_id, 
                seq3.line as seq3_line, seq3.type as seq3_type, 
                seq5.line as seq5_line, seq5.type as seq5_type, 
                lnk.create_time, lnk.last_updated_time
            from
                dbsnp_{database_name}.snpsubsnplink as lnk
                left join dbsnp_{database_name}.contigloc_snp_ids_all_builds all_snps on lnk.snp_id = all_snps.snp_id
                join dbsnp_{database_name}.subsnpseq3 as seq3 on lnk.subsnp_id = seq3.subsnp_id
                join dbsnp_{database_name}.subsnpseq5 as seq5 on lnk.subsnp_id = seq5.subsnp_id
            where all_snps.snp_id is null
        );
        """.format(**species_info)

    grant_statement = "grant select on dbsnp_{database_name}.unmapped_variants_all_builds " \
                      "to dbsnp_ro;".format(**species_info)

    return (os.linesep*2).join([work_mem_clause, temp_table_creation_query, subsnpseq_index_query,
                                textwrap.dedent(contigloc_snp_id_query + unmapped_variants_report_query),
                                grant_statement])


def write_report_query_file(species_info):
    species_info["report_query_file"] = os.path.sep.join([species_info["report_folder"],
                                                          "unmapped_variants_query.sql"])
    with open(species_info["report_query_file"], "w") as report_query_file_handle:
        report_query_file_handle.write(get_report_creation_query(species_info).lstrip())


def run_report_for_species(species_info):
    if "contigloc_table_list" in species_info:
        os.makedirs(species_info["report_folder"], exist_ok=True)
        logger.info("Creating report for species: {database_name} in the folder {report_folder}"
                    .format(**species_info))
        write_report_query_file(species_info)

        dependency_argument_for_bsub = ""
        # Only run one job at a time in a given Postgres host machine
        if species_info["pg_host"] in prev_job_in_host:
            dependency_argument_for_bsub = '-w "ended({0})" '.format(prev_job_in_host[species_info["pg_host"]])
        prev_job_in_host[species_info["pg_host"]] = species_info["bsub_job_name"]

        bsub_command_to_run = ("cd {report_folder} && bsub " + dependency_argument_for_bsub
                               + "-J {bsub_job_name} "
                                 "-o {bsub_job_name}.log "
                                 "-e {bsub_job_name}.err "
                               + "psql -U dbsnp -h {pg_host} -p {pg_port} -d dbsnp_{dbsnp_build} "
                                 "-f {report_query_file} -v ON_ERROR_STOP=1").format(**species_info)

        with open(final_command_file, "a") as command_file_handle:
            command_file_handle.write(bsub_command_to_run + os.linesep)


def check_prerequisites_for_species(species_info):
    if not does_subsnpseq_table_exist_for_species(species_info):
        logger.error("Species {0} does not have one or both of the tables: SUBSNPSEQ3 and SUBSNPSEQ5"
                     .format(species_info["database_name"]))
    contigloc_table_list = get_contigloc_table_list_for_schema(species_info)
    if len(contigloc_table_list) == 0:
        logger.error("No Contigloc table found for species {0}".format(species_info["database_name"]))
    else:
        species_info["contigloc_table_list"] = contigloc_table_list


def main(command_line_args):
    curr_timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    species_info = data_ops.get_species_pg_conn_info(command_line_args.metadb, command_line_args.metauser,
                                                     command_line_args.metahost)
    # Sort species connection information to facilitate parallel bsub invocation on multiple databases
    species_info_sorted_by_host = sorted(species_info, key=lambda species: (species["pg_host"],
                                                                            -1 * int(species["dbsnp_build"][:3]),
                                                                            species["database_name"]))

    for species_info in species_info_sorted_by_host:
        logger.info("Processing species: " + species_info["database_name"])
        species_info["report_folder"] = os.path.sep.join([command_line_args.eva_folder, species_info["database_name"],
                                                         "unmapped_variants_report"])
        species_info["bsub_job_name"] = species_info["database_name"] + "_run_unmapped_variants_" + curr_timestamp

        check_prerequisites_for_species(species_info)
        run_report_for_species(species_info)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate unmapped variants data for all species', add_help=False)
    parser.add_argument("-d", "--metadb", help="Postgres metadata DB", required=True)
    parser.add_argument("-u", "--metauser", help="Postgres metadata DB username", required=True)
    parser.add_argument("-h", "--metahost", help="Postgres metadata DB host", required=True)
    parser.add_argument("-o", "--eva-folder", help="Top level EVA folder (per-species folders will be created "
                                                   "for the report here", required=True)
    parser.add_argument('--help', action='help', help='Show this help message and exit')

    args = {}
    prev_job_in_host = {}

    final_command_file = "bsub_commands_to_run.sh"
    try:
        os.remove(final_command_file)
    except OSError:
        pass

    try:
        args = parser.parse_args()
        main(args)
    except Exception as ex:
        logger.exception(ex)
