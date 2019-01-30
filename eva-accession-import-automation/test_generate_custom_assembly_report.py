import json
import unittest
from generate_custom_assembly_report import *


class TestGenerateCustomAssemblyReport(unittest.TestCase):
    def setupConfig(self, config_file):
        with open(config_file) as json_config_file_handle:
            self.config = json.load(json_config_file_handle)
        os.environ["PGPASSFILE"] = self.config["PGPASSFILE"]
        self.species_db_info = filter(lambda x: x["database_name"] == self.config["species"],
                              data_ops.get_species_pg_conn_info(self.config["metadb"], self.config["metauser"],
                                                                self.config["metahost"]))[0]
        self.assembly_report_path = download_assembly_report(self.config["assembly_accession"])
        self.assembly_report_dataframe, self.genbank_equivalents_dataframe = \
            get_dataframe_for_assembly_report(self.assembly_report_path), \
            get_dataframe_for_genbank_equivalents(self.config["genbank_equivalents_file"],
                                                  self.config["assembly_accession"])

    def test_get_refseq_accessions_without_equivalent_genbank_accessions(self):
        self.setupConfig("test-config-bony-fish-7950.json")
        self.assertEqual({"NC_009577.1"},
                         get_refseq_accessions_without_equivalent_genbank_accessions(self.assembly_report_dataframe))
        self.setupConfig("test-config-carrot-79200.json")
        self.assertEqual({"NC_008325.1", "NC_017855.1"},
                         get_refseq_accessions_without_equivalent_genbank_accessions(self.assembly_report_dataframe))

    def test_get_genbank_accessions_without_equivalent_refseq_accessions(self):
        self.setupConfig("test-config-bony-fish-7950.json")
        self.assertEqual(set([]),
                         get_genbank_accessions_without_equivalent_refseq_accessions(self.assembly_report_dataframe))
        self.setupConfig("test-config-carrot-79200.json")
        self.assertEqual({"CM004358.1", "LNRQ01000010.1"},
                         get_genbank_accessions_without_equivalent_refseq_accessions(self.assembly_report_dataframe))

    def test_get_absent_refseq_accessions(self):
        self.setupConfig("test-config-bony-fish-7950.json")
        self.assertEqual([], get_absent_refseq_accessions(self.species_db_info,
                                                          set(self.assembly_report_dataframe["RefSeq-Accn"])))
        self.setupConfig("test-config-carrot-79200.json")
        self.assertEqual({'NW_016089419.1', 'NW_016089421.1', 'NW_016089420.1', 'NW_016089418.1', 'NW_016089417.1',
                          'NW_016089423.1', 'NW_016089416.1', 'NW_016089424.1', 'NW_016089422.1'},
                         set([x[0] for x in
                              get_absent_refseq_accessions(self.species_db_info,
                                                           set(self.assembly_report_dataframe["RefSeq-Accn"]))]))

    def test_insert_absent_genbank_accessions_in_assembly_report(self):
        self.setupConfig("test-config-bony-fish-7950.json")
        dataframe_num_entries_before_insert = len(self.assembly_report_dataframe)
        self.assembly_report_dataframe = \
            insert_absent_genbank_accessions_in_assembly_report(self.species_db_info,
                                                                self.assembly_report_dataframe,
                                                                self.genbank_equivalents_dataframe)
        dataframe_num_entries_after_insert = len(self.assembly_report_dataframe)
        self.assertEqual(dataframe_num_entries_before_insert, dataframe_num_entries_after_insert)

        self.setupConfig("test-config-carrot-79200.json")
        dataframe_num_entries_before_insert = len(self.assembly_report_dataframe)
        self.assembly_report_dataframe = \
            insert_absent_genbank_accessions_in_assembly_report(self.species_db_info,
                                                                self.assembly_report_dataframe,
                                                                self.genbank_equivalents_dataframe)
        dataframe_num_entries_after_insert = len(self.assembly_report_dataframe)
        self.assertEqual(dataframe_num_entries_before_insert + 9, dataframe_num_entries_after_insert)

    def test_update_non_equivalent_genbank_accessions_in_assembly_report(self):
        self.setupConfig("test-config-bony-fish-7950.json")
        self.assembly_report_dataframe = \
            update_non_equivalent_genbank_accessions_in_assembly_report(self.assembly_report_dataframe,
                                                                        self.genbank_equivalents_dataframe)
        self.assertEqual("AP009133.1",
                         self.assembly_report_dataframe[self.assembly_report_dataframe["RefSeq-Accn"] == "NC_009577.1"]
                         ["GenBank-Accn"].values.tolist()[0])

        self.setupConfig("test-config-carrot-79200.json")
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
        self.setupConfig("test-config-carrot-79200.json")
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