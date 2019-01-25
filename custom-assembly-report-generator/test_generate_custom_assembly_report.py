import json
import unittest
from generate_custom_assembly_report import *

class TestGenerateCustomAssemblyReport(unittest.TestCase):
    def setUp(self):
        with open("test-config.json") as json_config_file_handle:
            self.config = json.load(json_config_file_handle)
        self.assembly_report_path = download_assembly_report(self.config["assembly_accession"])
        self.assembly_report_dataframe, self.genbank_equivalents_dataframe = \
            get_dataframe_for_assembly_report(self.assembly_report_path), \
            get_dataframe_for_genbank_equivalents(self.config["genbank_equivalents_file"],
                                                  self.config["assembly_accession"])

    def test_get_refseq_accessions_without_equivalent_genbank_accessions(self):
        self.assertEqual({"NC_018554.1"},
                         get_refseq_accessions_without_equivalent_genbank_accessions(self.assembly_report_dataframe))
