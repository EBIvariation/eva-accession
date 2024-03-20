import os
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.orm import Session

from gather_clustering_counts.gather_release_counts import find_link, ReleaseCounter
from gather_clustering_counts.release_count_models import RSCountPerTaxonomy


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

    def test_write_counts_to_db(self):
        """This test require a postgres database running on localhost. See config_xml_file.xml for detail."""
        log_files_release1 = [os.path.join(self.resource_folder, 'count_for_release1.log')]
        log_files_release2 = [os.path.join(self.resource_folder, 'count_for_release2.log')]
        list_cow_assemblies = ['GCA_000003055.3', 'GCA_000003055.5', 'GCA_000003205.1', 'GCA_000003205.4', 'GCA_000003205.6']
        with patch.object(ReleaseCounter, 'get_taxonomy_and_scientific_name') as ptaxonomy:
            ptaxonomy.return_value = (9913, 'Bos taurus')
            counter = ReleaseCounter(self.private_config_xml_file, config_profile=self.config_profile, release_version=1,
                                     logs=log_files_release1)
            counter.write_counts_to_db()
            session = Session(counter.sqlalchemy_engine)
            query = select(RSCountPerTaxonomy).where(RSCountPerTaxonomy.taxonomy_id == 9913,
                                                     RSCountPerTaxonomy.rs_type == 'current',
                                                     RSCountPerTaxonomy.release_version == 1)
            result = session.execute(query).fetchone()
            rs_taxonomy_count = result.RSCountPerTaxonomy
            assert sorted(rs_taxonomy_count.assembly_accessions) == list_cow_assemblies
            assert rs_taxonomy_count.count == 169904286
            assert rs_taxonomy_count.new == 0

        with patch.object(ReleaseCounter, 'get_taxonomy_and_scientific_name') as ptaxonomy:
            ptaxonomy.return_value = (9913, 'Bos taurus')
            counter = ReleaseCounter(self.private_config_xml_file, config_profile=self.config_profile, release_version=2,
                                     logs=log_files_release2)
            counter.write_counts_to_db()
            query = select(RSCountPerTaxonomy).where(RSCountPerTaxonomy.taxonomy_id == 9913,
                                                     RSCountPerTaxonomy.rs_type == 'current',
                                                     RSCountPerTaxonomy.release_version == 2)
            result = session.execute(query).fetchone()
            rs_taxonomy_count = result.RSCountPerTaxonomy
            assert sorted(rs_taxonomy_count.assembly_accessions) == list_cow_assemblies
            assert rs_taxonomy_count.count == 169101573
            assert rs_taxonomy_count.new == -802713
