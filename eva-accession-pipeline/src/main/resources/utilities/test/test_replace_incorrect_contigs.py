from unittest import TestCase

from ebi_eva_common_pyutils.mongodb import MongoDatabase

from replace_incorrect_contigs_in_accessioning_warehouse import replace_with_correct_contig


class TestCorrectContigErrorInStudy(TestCase):
    def setUp(self) -> None:
        self.contig = "AF034253.1"
        self.db = "eva_accession_sharded_test"
        self.collection = "submittedVariantEntity"
        self.uri = "mongodb://localhost:27017/"
        self.mongo_source = MongoDatabase(uri=self.uri, db_name=self.db)
        wrong_contig = [{
            "_id": "1125697507941CA420E26588F9F40F6C56C876A0",
            "accession": 7315407067,
            "alt": "G",
            "contig": "M",
            "createdDate": "2021-02-24T10:26:17.561Z",
            "ref": "A",
            "seq": "GCA_000003025.4",
            "start": 158,
            "study": "PRJEB43246",
            "tax": 9823,
            "version": 1
        },
            {
                "_id": "6CD16D81C36466B1C12A4D1911DAD1A7ECDA0976",
                "accession": 7315401731,
                "alt": "T",
                "contig": "CM000812.4",
                "createdDate": "2021-02-24T10:25:25.259Z",
                "ref": "C",
                "seq": "GCA_000003025.4",
                "start": 21664,
                "study": "PRJEB43246",
                "tax": 9823,
                "version": 1
            }
        ]

        self.mongo_source.mongo_handle[self.db][self.collection].drop()
        self.mongo_source.mongo_handle[self.db][self.collection].insert_many(wrong_contig)

    def tearDown(self) -> None:
        self.mongo_source.mongo_handle[self.db][self.collection].drop()
        self.mongo_source.mongo_handle.close()

    def test_replace_with_correct_contig(self):
        assembly_accession = "GCA_000003025.4"
        study_accession = "PRJEB43246"
        incorrect_contig = "M"
        correct_contig = "AF034253.1"
        unrelated_contig = "CM000812.4"
        fixed = replace_with_correct_contig(self.mongo_source, assembly_accession, study_accession, incorrect_contig,
                                            correct_contig, 1)
        self.assertEqual(fixed, 1)
        variant = (self.mongo_source.mongo_handle[self.db][self.collection].find_one(
            {'seq': assembly_accession, 'accession': 7315407067}))
        self.assertEqual(variant['contig'], self.contig)
        self.assertEqual(variant['_id'], '5F9B885F7A177A38A5AD0D0DEDBD3967F684DD6B')
        variant1 = (self.mongo_source.mongo_handle[self.db][self.collection].find_one(
            {'_id': '1125697507941CA420E26588F9F40F6C56C876A0'}))
        self.assertIsNone(variant1)
        variant2 = (self.mongo_source.mongo_handle[self.db][self.collection].find_one(
            {'_id': '6CD16D81C36466B1C12A4D1911DAD1A7ECDA0976', 'contig': unrelated_contig}))
        self.assertIsNotNone(variant2)
