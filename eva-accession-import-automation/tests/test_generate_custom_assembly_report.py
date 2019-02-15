# Copyright 2018 EMBL - European Bioinformatics Institute
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

import json
import unittest
from generate_custom_assembly_report import *


class TestGenerateCustomAssemblyReport(unittest.TestCase):
    def setupTestConfig(self, config_file):
        with open(config_file) as json_config_file_handle:
            self.config = json.load(json_config_file_handle)
        self.species = self.config["species"]
        self.refseq_accessions_from_db = set(self.config["refseq_accessions_from_db"])
        self.assembly_report_path = self.config["assembly_report_path"]
        self.assembly_report_dataframe, self.genbank_equivalents_dataframe = \
            get_dataframe_for_assembly_report(self.assembly_report_path), \
            get_dataframe_for_genbank_equivalents(self.config["genbank_equivalents_file"])

    def test_get_refseq_accessions_without_equivalent_genbank_accessions(self):
        self.setupTestConfig("tests/config/test-config-bony-fish-7950.json")
        self.assertEqual({"NC_009577.1"},
                         get_refseq_accessions_without_equivalent_genbank_accessions(self.assembly_report_dataframe))
        self.setupTestConfig("tests/config/test-config-carrot-79200.json")
        self.assertEqual({"NC_008325.1", "NC_017855.1"},
                         get_refseq_accessions_without_equivalent_genbank_accessions(self.assembly_report_dataframe))

    def test_get_genbank_accessions_without_equivalent_refseq_accessions(self):
        self.setupTestConfig("tests/config/test-config-bony-fish-7950.json")
        self.assertEqual(set([]),
                         get_genbank_accessions_without_equivalent_refseq_accessions(self.assembly_report_dataframe))
        self.setupTestConfig("tests/config/test-config-carrot-79200.json")
        self.assertEqual({"CM004358.1", "LNRQ01000010.1"},
                         get_genbank_accessions_without_equivalent_refseq_accessions(self.assembly_report_dataframe))

    def test_get_absent_refseq_accessions(self):
        self.setupTestConfig("tests/config/test-config-bony-fish-7950.json")
        self.assertEqual({'NW_012224383.1', 'NW_012224352.1', 'NW_012224347.1'},
                         get_absent_refseq_accessions(self.refseq_accessions_from_db,
                                                      set(self.assembly_report_dataframe["RefSeq-Accn"])))
        self.setupTestConfig("tests/config/test-config-carrot-79200.json")
        self.assertEqual({'NW_016090857.1', 'NW_016090852.1'},
                         get_absent_refseq_accessions(self.refseq_accessions_from_db,
                                                      set(self.assembly_report_dataframe["RefSeq-Accn"])))

    def test_insert_absent_genbank_accessions_in_assembly_report(self):
        self.setupTestConfig("tests/config/test-config-bony-fish-7950.json")
        dataframe_num_entries_before_insert = len(self.assembly_report_dataframe)
        self.assembly_report_dataframe = \
            insert_absent_genbank_accessions_in_assembly_report(self.species, self.refseq_accessions_from_db,
                                                                self.assembly_report_dataframe,
                                                                self.genbank_equivalents_dataframe)
        dataframe_num_entries_after_insert = len(self.assembly_report_dataframe)
        self.assertEqual(dataframe_num_entries_before_insert + 3, dataframe_num_entries_after_insert)

        self.setupTestConfig("tests/config/test-config-carrot-79200.json")
        dataframe_num_entries_before_insert = len(self.assembly_report_dataframe)
        self.assembly_report_dataframe = \
            insert_absent_genbank_accessions_in_assembly_report(self.species, self.refseq_accessions_from_db,
                                                                self.assembly_report_dataframe,
                                                                self.genbank_equivalents_dataframe)
        dataframe_num_entries_after_insert = len(self.assembly_report_dataframe)
        self.assertEqual(dataframe_num_entries_before_insert + 2, dataframe_num_entries_after_insert)

    def test_update_non_equivalent_genbank_accessions_in_assembly_report(self):
        self.setupTestConfig("tests/config/test-config-bony-fish-7950.json")
        self.assembly_report_dataframe = \
            update_non_equivalent_genbank_accessions_in_assembly_report(self.assembly_report_dataframe,
                                                                        self.genbank_equivalents_dataframe)
        self.assertEqual("AP009133.1",
                         self.assembly_report_dataframe[self.assembly_report_dataframe["RefSeq-Accn"] == "NC_009577.1"]
                         ["GenBank-Accn"].values.tolist()[0])

        self.setupTestConfig("tests/config/test-config-carrot-79200.json")
        self.assembly_report_dataframe = \
            update_non_equivalent_genbank_accessions_in_assembly_report(self.assembly_report_dataframe,
                                                                        self.genbank_equivalents_dataframe)
        self.assertEqual("JQ248574.1",
                         self.assembly_report_dataframe[self.assembly_report_dataframe["RefSeq-Accn"] == "NC_017855.1"]
                         ["GenBank-Accn"].values.tolist()[0])
        self.assertEqual("DQ898156.1",
                         self.assembly_report_dataframe[self.assembly_report_dataframe["RefSeq-Accn"] == "NC_008325.1"]
                         ["GenBank-Accn"].values.tolist()[0])

    def test_update_non_equivalent_refseq_accessions_in_assembly_report(self):
        self.setupTestConfig("tests/config/test-config-carrot-79200.json")
        self.assembly_report_dataframe = \
            update_non_equivalent_genbank_accessions_in_assembly_report(self.assembly_report_dataframe,
                                                                        self.genbank_equivalents_dataframe)
        self.assertEqual("na",
                         self.assembly_report_dataframe[self.assembly_report_dataframe["GenBank-Accn"]
                                                        == "CM004358.1"]
                         ["RefSeq-Accn"].values.tolist()[0])
        self.assertEqual("na",
                         self.assembly_report_dataframe[self.assembly_report_dataframe["GenBank-Accn"]
                                                        == "LNRQ01000010.1"]
                         ["RefSeq-Accn"].values.tolist()[0])


if __name__ == '__main__':
    unittest.main()
