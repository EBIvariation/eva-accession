import os
from itertools import cycle
from unittest import TestCase
from unittest.mock import patch

from ebi_eva_common_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_common_pyutils.pg_utils import execute_query
from sqlalchemy import select
from sqlalchemy.orm import Session

from gather_clustering_counts.gather_release_counts import find_link, ReleaseCounter
from gather_clustering_counts.release_count_models import RSCountPerTaxonomy, RSCountPerAssembly, RSCountCategory, \
    RSCount


def test_find_links():
    d1 = {
        'A': ['1', '2'],
        'B': ['2', '5'],
        'C': ['3', '4'],
        'D': ['5'],
        'E': []
    }
    d2 = {
        '1': ['A', 'B'],
        '2': ['A'],
        '3': ['C'],
        '4': ['C'],
        '5': ['D', 'B']
    }
    assert find_link({'A'}, d1, d2) == (frozenset({'A', 'B', 'D'}), frozenset({'1', '2', '5'}))
    assert find_link({'B'}, d1, d2) == (frozenset({'A', 'B', 'D'}), frozenset({'1', '2', '5'}))
    assert find_link({'C'}, d1, d2) == (frozenset({'C'}), frozenset({'3', '4'}))
    assert find_link({'D'}, d1, d2) == (frozenset({'A', 'B', 'D'}), frozenset({'1', '2', '5'}))
    assert find_link({'E'}, d1, d2) == (frozenset({'E'}), frozenset({}))



class TestReleaseCounter(TestCase):

    resource_folder = os.path.dirname(__file__)

    def setUp(self):
        self.private_config_xml_file = os.path.join(self.resource_folder, 'config_xml_file.xml')
        self.config_profile = "localhost"
        with get_metadata_connection_handle(self.config_profile, self.private_config_xml_file) as db_conn:
            for sqlalchemy_class in [RSCountCategory, RSCount, RSCountPerTaxonomy, RSCountPerAssembly]:
                query = f'DROP TABLE {sqlalchemy_class.schema}.{sqlalchemy_class.__tablename__}'
                execute_query(db_conn, query)

    def test_write_counts_to_db(self):
        """This test require a postgres database running on localhost. See config_xml_file.xml for detail."""
        log_files_release1 = [os.path.join(self.resource_folder, 'count_for_release1.log')]
        log_files_release2 = [os.path.join(self.resource_folder, 'count_for_release2.log')]
        list_cow_assemblies = ['GCA_000003055.3', 'GCA_000003055.5', 'GCA_000003205.1', 'GCA_000003205.4', 'GCA_000003205.6', 'Unmapped']
        folder_to_taxonomy = {'bos_taurus': 9913}

        with patch.object(ReleaseCounter, 'get_taxonomy') as ptaxonomy:
            # ptaxonomy.side_effect = lambda x: folder_to_taxonomy.get(x)
            ptaxonomy.return_value = 9913
            counter = ReleaseCounter(self.private_config_xml_file, config_profile=self.config_profile,
                                     release_version=1, logs=log_files_release1)
            counter.write_counts_to_db()
            counter = ReleaseCounter(self.private_config_xml_file, config_profile=self.config_profile,
                                     release_version=2, logs=log_files_release2)
            counter.write_counts_to_db()

        session = Session(counter.sqlalchemy_engine)
        query = select(RSCountPerTaxonomy).where(RSCountPerTaxonomy.taxonomy_id == 9913,
                                                 RSCountPerTaxonomy.release_version == 1)
        result = session.execute(query).fetchone()
        rs_taxonomy_count = result.RSCountPerTaxonomy
        assert sorted(rs_taxonomy_count.assembly_accessions) == list_cow_assemblies
        assert rs_taxonomy_count.current_rs == 102813585
        assert rs_taxonomy_count.new_current_rs == 0
        assert rs_taxonomy_count.release_folder == 'Cow_9913'

        query = select(RSCountPerTaxonomy).where(RSCountPerTaxonomy.taxonomy_id == 9913,
                                                 RSCountPerTaxonomy.release_version == 2)
        result = session.execute(query).fetchone()
        rs_taxonomy_count = result.RSCountPerTaxonomy
        assert sorted(rs_taxonomy_count.assembly_accessions) == list_cow_assemblies
        assert rs_taxonomy_count.current_rs == 102605893
        assert rs_taxonomy_count.new_current_rs == -207692
        assert rs_taxonomy_count.release_folder == 'bos_taurus'

        query = select(RSCountPerAssembly).where(RSCountPerAssembly.assembly_accession == 'GCA_000003205.6',
                                                 RSCountPerAssembly.release_version == 1)
        result = session.execute(query).fetchone()
        rs_assembly_count = result.RSCountPerAssembly
        assert sorted(rs_assembly_count.taxonomy_ids) == [9913]
        assert rs_assembly_count.current_rs == 61038394
        assert rs_assembly_count.new_current_rs == 0
        assert rs_assembly_count.release_folder == 'GCA_000003205.6'

    def test_write_counts_to_db2(self):
        """This test require a postgres database running on localhost. See config_xml_file.xml for detail."""
        log_files_release = [os.path.join(self.resource_folder, 'count_for_haplochromini_oreochromis_niloticus.log')]
        folder_to_taxonomy = {'oreochromis_niloticus': 8128, 'haplochromini': 319058}

        with patch.object(ReleaseCounter, 'get_taxonomy') as ptaxonomy:
            ptaxonomy.side_effect = lambda x: folder_to_taxonomy.get(x)
            counter = ReleaseCounter(self.private_config_xml_file, config_profile=self.config_profile,
                                     release_version=4, logs=log_files_release)
            counter.write_counts_to_db()
            session = Session(counter.sqlalchemy_engine)

            query = select(RSCountPerAssembly).where(RSCountPerAssembly.assembly_accession == 'GCA_000188235.2',
                                                     RSCountPerAssembly.release_version == 4)
            result = session.execute(query).fetchone()
            rs_assembly_count = result.RSCountPerAssembly
            assert rs_assembly_count.current_rs == 18747108  # 18746871 + 237
            assert rs_assembly_count.release_folder == 'GCA_000188235.2'

