from unittest import TestCase

from sqlalchemy.orm import Session

from gather_clustering_counts.release_count_models import RSCount, RSCountCategory, get_sql_alchemy_engine


class TestModels(TestCase):

    def setUp(self):
        # engine = create_engine('psycopg2://')
        # self.session = Session(engine)
        # # Create an in memory database that emulate the postgres' schema
        # self.session.execute(text("ATTACH DATABASE ':memory:' AS eva_stats"))
        # # Create all tables:
        # release_count_models.Base.metadata.create_all(engine)
        engine = get_sql_alchemy_engine(
            dbtype='postgresql',
            username='test',
            password='test',
            host_url='localhost',
            database='evapro',
            port='5432'
        )
        self.session = Session(engine)
        self._add_data()

    def _add_data(self):
        rs_count = RSCount(count=1, group_description='group_description1')
        rs_category = RSCountCategory(assembly='GCA_0000001.1', taxonomy=10000, rs_type='current', release_version=1,
                                      rs_count=rs_count)
        self.session.add(rs_count)
        self.session.add(rs_category)
        self.session.flush()

    def tearDown(self):
        pass

    def test_use_views(self):
        pass
