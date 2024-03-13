import sqlalchemy
from sqlalchemy import MetaData, Column, Integer, String, ForeignKey, UniqueConstraint, BigInteger, TEXT, create_engine, \
    URL, text, schema, inspect
from sqlalchemy.orm import declarative_base, mapped_column, relationship


metadata = MetaData(schema="eva_stats")
Base = declarative_base(metadata=metadata)


def _pluralise(name):
    """Only works with name ending in Y"""
    return f'{name[:-1]}ies'


drop_view_template = """DROP VIEW eva_stats.release_rs_count_per_{aggregate}"""


create_view_template = """CREATE VIEW eva_stats.release_rs_count_per_{aggregate} AS
WITH count_per_{aggregate} AS (
    SELECT {aggregate}, rs_type, release_version, ARRAY_AGG(DISTINCT {source}) AS {_pluralise(source)}, SUM(count) AS count 
    FROM eva_stats.release_rs_count_category cc 
    JOIN eva_stats.release_rs_count c ON c.rs_count_id=cc.rs_count_id 
    GROUP BY {aggregate}, release_version, rs_type
)
SELECT current.{aggregate} AS {aggregate}, current.rs_type AS rs_Type, current.release_version AS release_version, 
       current.{_pluralise(source)} as {_pluralise(source)} current.count AS count, coalesce(current.count-previous.count, 0) as new 
FROM count_per_{aggregate} AS current
LEFT JOIN count_per_{aggregate} AS previous
ON current.release_version=previous.release_version+1 AND current.{aggregate}=previous.{aggregate} AND current.rs_type=previous.rs_type;
"""


def create_views_from_sql(engine):
    for aggregate, source in [('taxonomy', 'assembly'), ('assembly', 'taxonomy')]:
        with engine.begin() as conn:
            try:
                conn.execute(text(drop_view_template.format(aggregate=aggregate)))
            except sqlalchemy.exc.ProgrammingError:
                pass
        with engine.begin() as conn:
            conn.execute(text(create_view_template.format(aggregate=aggregate, source=source)))


class RSCountCategory(Base):
    """
    Table that provide the metadata associated with a number of RS id
    """
    __tablename__ = 'release_rs_count_category'

    count_category_id = Column(Integer, primary_key=True, autoincrement=True)
    assembly = Column(String)
    taxonomy = Column(Integer)
    rs_type = Column(String)
    release_version = Column(Integer)
    rs_count_id = mapped_column(ForeignKey("release_rs_count.rs_count_id"))
    rs_count = relationship("RSCount", back_populates="count_categories")
    __table_args__ = (
        UniqueConstraint('assembly', 'taxonomy', 'rs_type', 'release_version', 'rs_count_id', name='uix_1'),
    )
    schema = 'eva_stats'


class RSCount(Base):
    """
    Table that provide the count associated with each category
    """
    __tablename__ = 'release_rs_count'
    rs_count_id = Column(Integer, primary_key=True, autoincrement=True)
    count = Column(BigInteger)
    group_description = Column(TEXT, unique=True)
    count_categories = relationship("RSCountCategory", back_populates="rs_count")
    schema = 'eva_stats'


def get_sql_alchemy_engine(dbtype, username, password, host_url, database, port):
    engine = create_engine(URL.create(
        dbtype + '+psycopg2',
        username=username,
        password=password,
        host=host_url.split('/')[-1],
        database=database,
        port=port
    ))
    with engine.begin() as conn:
        conn.execute(schema.CreateSchema(RSCount.schema, if_not_exists=True))
    RSCount.__table__.create(bind=engine, checkfirst=True)
    RSCountCategory.__table__.create(bind=engine, checkfirst=True)
    create_views_from_sql(engine)
    return engine



