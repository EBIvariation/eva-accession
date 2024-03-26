#!/usr/bin/env python
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
import argparse
from collections import defaultdict
from functools import cached_property
from itertools import cycle

from ebi_eva_common_pyutils.assembly import NCBIAssembly
from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile
from ebi_eva_common_pyutils.logger import logging_config, AppLogger
from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.mongodb import MongoDatabase
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, execute_query
from ebi_eva_common_pyutils.taxonomy.taxonomy import normalise_taxon_scientific_name, get_scientific_name_from_ensembl


# round-robin through the instances from 1 to 10
tempmongo_instances = cycle([f'tempmongo-{instance}' for instance in range(1, 11)])

all_tasks = ['fill_release_entries', 'fill_should_be_released']


class ReleaseTracker(AppLogger):

    def __init__(self, private_config_xml_file, maven_profile, release_version, reference_directory):
        self.private_config_xml_file = private_config_xml_file
        self.maven_profile = maven_profile
        self.release_version = release_version
        self.ref_dir = reference_directory

    @cached_property
    def metadata_conn(self):
        return get_metadata_connection_handle(self.maven_profile, self.private_config_xml_file)

    @cached_property
    def mongo_conn(self):
        mongo_uri = get_mongo_uri_for_eva_profile(self.maven_profile, self.private_config_xml_file)
        return MongoDatabase(uri=mongo_uri, db_name="eva_accession_sharded")

    def create_table_if_not_exists(self):
        query_create_table = (
            'create table if not exists eva_progress_tracker.clustering_release_tracker('
            'taxonomy int4 not null, '
            'scientific_name text not null, '
            'assembly_accession text not null, '
            'release_version int8 not null, '
            'sources text not null,'
            'clustering_status text null, '       # unused
            'clustering_start timestamp null, '   # unused
            'clustering_end timestamp null, '     # unused
            'should_be_clustered boolean null, '  # unused
            'fasta_path text null, '
            'report_path text null, '
            'tempmongo_instance text null, '
            'should_be_released boolean null, '
            'num_rs_to_release int8 null, '       # not computed but still used by release automation
            'total_num_variants int8 null, '      # not computed and unused
            'release_folder_name text null, '
            'release_status text null, '
            'primary key (taxonomy, assembly_accession, release_version))'
        )
        execute_query(self.metadata_conn, query_create_table)

    def fill_release_entries(self):
        """Fill in release table based on previous release data, EVA metadata, and supported assembly tracker.
        Also fills in should_be_released values."""
        self._fill_from_previous_release()
        self._fill_from_eva_metadata()
        self._fill_from_supported_assembly_tracker()
        self.fill_should_be_released_for_all()

    def _fill_from_previous_release(self):
        query = f"""select taxonomy, scientific_name, assembly_accession, sources, fasta_path, report_path, 
                    release_folder_name from eva_progress_tracker.clustering_release_tracker 
                    where release_version = {self.release_version - 1}"""
        for tax, sc_name, asm_acc, src, fs_path, rpt_path, rls_folder_name in get_all_results_for_query(
                self.metadata_conn, query):
            self._insert_entry_for_taxonomy_and_assembly(tax, asm_acc, src, sc_name, fs_path, rpt_path,
                                                         rls_folder_name)

    def _fill_from_eva_metadata(self):
        query = f"""select distinct  pt.taxonomy_id as taxonomy, asm.assembly_accession as assembly_accession
                    from evapro.project_taxonomy pt
                    join evapro.project_analysis pa on pt.project_accession = pa.project_accession 
                    join evapro.analysis a on a.analysis_accession = pa.analysis_accession 
                    join evapro.assembly asm on asm.assembly_set_id = a.assembly_set_id 
                    and asm.assembly_accession is not null and assembly_accession like 'GCA%'"""
        sources = 'EVA'
        for tax, asm_acc in get_all_results_for_query(self.metadata_conn, query):
            self._insert_entry_for_taxonomy_and_assembly(tax, asm_acc, sources)

    def _fill_from_supported_assembly_tracker(self):
        query = f"""select distinct taxonomy_id as taxonomy, assembly_id as assembly_accession
                    from evapro.supported_assembly_tracker"""
        sources = 'DBSNP, EVA'
        for tax, asm_acc in get_all_results_for_query(self.metadata_conn, query):
            self._insert_entry_for_taxonomy_and_assembly(tax, asm_acc, sources)

    def _insert_entry_for_taxonomy_and_assembly(self, tax, asm_acc, sources, sc_name=None, fasta_path=None,
                                                report_path=None, release_folder_name=None):
        sc_name = sc_name if sc_name else get_scientific_name_from_ensembl(tax)
        sc_name = sc_name.replace("'", "\''")
        if asm_acc != 'Unmapped':
            ncbi_assembly = NCBIAssembly(asm_acc, sc_name, self.ref_dir)
        fasta_path = fasta_path if fasta_path else ncbi_assembly.assembly_fasta_path
        report_path = report_path if report_path else ncbi_assembly.assembly_report_path
        release_folder_name = release_folder_name if release_folder_name else normalise_taxon_scientific_name(sc_name)

        tempongo_instance = next(tempmongo_instances)
        src_in_db = self.get_sources_for_taxonomy_assembly(tax, asm_acc)

        if not src_in_db:
            # entry does not exist for tax and asm
            insert_query = f"""INSERT INTO eva_progress_tracker.clustering_release_tracker(
                            taxonomy, scientific_name, assembly_accession, release_version, sources,
                            fasta_path, report_path, tempmongo_instance, release_folder_name) 
                            VALUES ({tax}, '{sc_name}', '{asm_acc}', {self.release_version}, '{sources}', 
                            '{fasta_path}', '{report_path}', '{tempongo_instance}', '{release_folder_name}') 
                            ON CONFLICT DO NOTHING"""
            execute_query(self.metadata_conn, insert_query)
        else:
            # if DB source is equal to what we are trying to insert or if the DB source already contains
            # both EVA and DBSNP, no need to insert again
            if src_in_db == sources or ('EVA' in src_in_db and 'DBSNP' in src_in_db):
                self.info(f"Entry already present for taxonomy {tax} and assembly {asm_acc} with sources {sources}")
            else:
                # We have different sources which means we need to update entry to have both DBNSP and EVA in sources
                update_query = f"""update eva_progress_tracker.clustering_release_tracker set sources='DBSNP, EVA'
                                where taxonomy={tax} and assembly_accession='{asm_acc}' and  
                                release_version={self.release_version}"""
                execute_query(self.metadata_conn, update_query)

    def fill_should_be_released_for_all(self):
        tax_asm = self.get_taxonomy_list_for_release()
        for tax in tax_asm:
            self.fill_should_be_released_for_taxonomy(tax, tax_asm[tax])

    def fill_should_be_released_for_taxonomy(self, tax, asm_sources):
        for asm_acc in asm_sources:
            self.fill_should_be_released_for_taxonomy_and_assembly(tax, asm_acc, asm_sources[asm_acc])

    def fill_should_be_released_for_taxonomy_and_assembly(self, tax, asm, src):
        """Fills should_be_released for a taxonomy/assembly pair based on whether there are current RS IDs,
        and updates the sources column as well.
        TODO Also check for deprecated (https://www.ebi.ac.uk/panda/jira/browse/EVA-3402)"""
        should_be_released_eva = should_be_released_dbsnp = False
        if asm != 'Unmapped':
            ss_query = {'tax': tax, 'seq': asm, 'rs': {'$exists': True}}
            rs_query = {'tax': tax, 'asm': asm}
            if 'EVA' in src:
                eva_ss_coll = 'submittedVariantEntity'
                eva_rs_coll = 'clusteredVariantEntity'
                should_be_released_eva = self._determine_should_be_released_for_collection(
                    tax, asm, eva_ss_coll, eva_rs_coll, ss_query, rs_query)
            if 'DBSNP' in src:
                dbsnp_ss_coll = 'dbsnpSubmittedVariantEntity'
                dbsnp_rs_coll = 'dbsnpClusteredVariantEntity'
                should_be_released_dbsnp = self._determine_should_be_released_for_collection(
                    tax, asm, dbsnp_ss_coll, dbsnp_rs_coll, ss_query, rs_query)
            should_be_released = should_be_released_eva or should_be_released_dbsnp
        else:
            should_be_released = False

        self.info(f"For taxonomy {tax} and assembly {asm}, should_be_released is {should_be_released} "
                  f"(should_be_released_eva = {should_be_released_eva}, "
                  f"should_be_released_dbsnp = {should_be_released_dbsnp})")
        num_rs_to_release = 1 if should_be_released else 0

        update_should_be_released_query = f"""update eva_progress_tracker.clustering_release_tracker 
                        set should_be_released={should_be_released}, num_rs_to_release={num_rs_to_release}
                        where taxonomy={tax} and assembly_accession='{asm}' and release_version={self.release_version}"""
        execute_query(self.metadata_conn, update_should_be_released_query)

        # for any taxonomy and assembly, if sources have both DBSNP and EVA but one of them does not have any variants
        # to release, then remove that from the sources
        if should_be_released and ('DBSNP' in src and 'EVA' in src):
            if not should_be_released_dbsnp or not should_be_released_eva:
                if should_be_released_eva:
                    self.info(f"For taxonomy {tax} and assembly {asm}, "
                              f"putting the source as EVA as DBSNP does not have any variants to release")
                    sources = 'EVA'
                elif should_be_released_dbsnp:
                    self.info(f"For taxonomy {tax} and assembly {asm}, "
                              f"putting the source as DBSNP as EVA does not have any variants to release")
                    sources = 'DBSNP'

                self.info(f"For tax {tax} and assembly {asm} Updating sources to {sources}")
                update_sources_query = f"""update eva_progress_tracker.clustering_release_tracker 
                            set sources='{sources}' where taxonomy={tax} and assembly_accession='{asm}' 
                            and release_version={self.release_version}"""
                execute_query(self.metadata_conn, update_sources_query)

    def _determine_should_be_released_for_collection(self, tax, asm, ss_coll, rs_coll, ss_query, rs_query):
        self.info(f"Looking for SS with RS for Taxonomy {tax} and Assembly {asm} in collection {ss_coll}")
        collection = self.mongo_conn.mongo_handle[self.mongo_conn.db_name][ss_coll]
        ss_with_rs = collection.find_one(ss_query)
        if ss_with_rs:
            self.info(f'Found SS with RS for Taxonomy {tax} and Assembly {asm} in collection {ss_coll}, SS: {ss_with_rs}')
            return True
        else:
            self.warning(f'No SS with RS found for Taxonomy {tax} and Assembly {asm} in collection {ss_coll}')

        # Looking for RS if no SS with RS is found, for cases where there might not be a variant in SS, but there might
        # be a RS in corresponding CVE collection.
        # (For release we will look up against both dbsnpSVE and SVE for records in a given EVA or dbSNP CVE collection
        # but only if we mark the sources in the release table, see below
        # https://github.com/EBIvariation/eva-accession/blob/5f827ae8f062ae923a83c16070f6ebf08c544e31/eva-accession-release/src/main/java/uk/ac/ebi/eva/accession/release/batch/io/active/AccessionedVariantMongoReader.java#L83))
        self.info(f"Looking for RS with Taxonomy {tax} and Assembly {asm} in collection {rs_coll}")
        collection = self.mongo_conn.mongo_handle[self.mongo_conn.db_name][rs_coll]
        rs_with_tax_asm = collection.find_one(rs_query)
        if rs_with_tax_asm:
            self.info(f'Found RS with Taxonomy {tax} and Assembly {asm} in collection {rs_coll}, RS: {rs_with_tax_asm}')
            return True
        else:
            self.warning(f'No RS found for Taxonomy {tax} and Assembly {asm} in collection {ss_coll}')
            return False

    def get_taxonomy_list_for_release(self):
        """Get all taxonomies with assemblies and sources for the current release version."""
        tax_asm = defaultdict(defaultdict)
        query = f"""select distinct taxonomy, assembly_accession, sources 
                    from eva_progress_tracker.clustering_release_tracker
                    where release_version={self.release_version}"""
        for tax, asm_acc, sources in get_all_results_for_query(self.metadata_conn, query):
            tax_asm[tax][asm_acc] = sources
        return tax_asm

    def get_assemblies_and_sources_for_taxonomy(self, taxonomy):
        assembly_source = {}
        query = f"""SELECT distinct assembly_accession, sources from eva_progress_tracker.clustering_release_tracker 
                    where taxonomy = {taxonomy} and release_version = {self.release_version}"""
        for assembly, sources in get_all_results_for_query(self.metadata_conn, query):
            assembly_source[assembly] = sources
        return assembly_source

    def get_sources_for_taxonomy_assembly(self, taxonomy, assembly):
        query = f"""SELECT sources from eva_progress_tracker.clustering_release_tracker 
                    where taxonomy = {taxonomy} and assembly_accession='{assembly}' 
                    and release_version = {self.release_version}"""
        result = get_all_results_for_query(self.metadata_conn, query)
        if not result:
            return None
        else:
            return result[0][0]


def main():
    parser = argparse.ArgumentParser(description='Create and load the clustering and release tracking table',
                                     add_help=False)
    parser.add_argument("--private-config-xml-file", required=True, help="ex: /path/to/eva-maven-settings.xml")
    parser.add_argument("--release-version", required=True, type=int, help="version of the release")
    parser.add_argument("--reference-directory", required=True,
                        help="Directory where the reference genomes exists or should be downloaded")
    parser.add_argument("--profile", required=True, help="Profile where entries should be made e.g. development")
    parser.add_argument('--tasks', required=False, type=str, nargs='+', choices=all_tasks,
                        help='Task or set of tasks to perform')
    parser.add_argument("--taxonomy", required=False, type=int,
                        help="taxonomy id for which should be released needs to be updated")
    parser.add_argument("--assembly", required=False,
                        help="assembly accession for which should be released needs to be updated")
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    args = parser.parse_args()

    logging_config.add_stdout_handler()

    if not args.tasks:
        args.tasks = ['fill_release_entries']

    release_tracker = ReleaseTracker(
        private_config_xml_file=args.private_config_xml_file,
        maven_profile=args.profile,
        release_version=args.release_version,
        reference_directory=args.reference_directory
    )

    release_tracker.create_table_if_not_exists()

    if 'fill_release_entries' in args.tasks:
        release_tracker.fill_release_entries()

    if 'fill_should_be_released' in args.tasks:
        if not args.taxonomy:
            raise Exception("For running task 'fill_should_be_released', it is mandatory to provide --taxonomy")
        if not args.assembly:
            asm_and_sources = release_tracker.get_assemblies_and_sources_for_taxonomy(args.taxonomy)
            release_tracker.fill_should_be_released_for_taxonomy(args.taxonomy, asm_and_sources)
        else:
            sources = release_tracker.get_sources_for_taxonomy_assembly(args.taxonomy, args.assembly)
            release_tracker.fill_should_be_released_for_taxonomy_and_assembly(args.taxonomy, args.assembly, sources)


if __name__ == '__main__':
    main()
