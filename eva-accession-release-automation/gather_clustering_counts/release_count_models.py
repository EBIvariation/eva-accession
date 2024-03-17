import sqlalchemy
from sqlalchemy import MetaData, Column, Integer, String, ForeignKey, UniqueConstraint, BigInteger, TEXT, create_engine, \
    URL, text, schema
from sqlalchemy.orm import declarative_base, mapped_column, relationship


metadata = MetaData(schema="eva_stats")
Base = declarative_base(metadata=metadata)


create_view_taxonomy = """CREATE OR REPLACE VIEW eva_stats.release_rs_count_per_taxonomy AS
WITH count_per_taxonomy AS (
    SELECT taxonomy_id, rs_type, release_version, ARRAY_AGG(DISTINCT assembly_accession) AS assembly_accessions, SUM(count) AS count 
    FROM eva_stats.release_rs_count_category cc 
    JOIN eva_stats.release_rs_count c ON c.rs_count_id=cc.rs_count_id 
    GROUP BY taxonomy_id, release_version, rs_type
)
SELECT current.taxonomy_id AS taxonomy_id, 
       t.scientific_name AS scientific_name, 
       t.common_name AS common_name, 
       current.rs_type AS rs_Type, 
       current.release_version AS release_version, 
       current.assembly_accessions as assembly_accessions, 
       current.count AS count, 
       coalesce(current.count-previous.count, 0) as new 
FROM count_per_taxonomy current
LEFT JOIN count_per_taxonomy previous
ON current.release_version=previous.release_version+1 AND current.taxonomy_id=previous.taxonomy_id AND current.rs_type=previous.rs_type
LEFT JOIN evapro.taxonomy t
ON current.taxonomy_id=t.taxonomy_id;

"""

create_view_assembly = """CREATE OR REPLACE VIEW eva_stats.release_rs_count_per_assembly AS
WITH count_per_assembly AS (
    SELECT assembly_accession, rs_type, release_version, ARRAY_AGG(DISTINCT taxonomy_id) AS taxonomy_ids, SUM(count) AS count 
    FROM eva_stats.release_rs_count_category cc 
    JOIN eva_stats.release_rs_count c ON c.rs_count_id=cc.rs_count_id 
    GROUP BY assembly_accession, release_version, rs_type
)
SELECT current.assembly_accession AS assembly_accession, 
       current.rs_type AS rs_Type, 
       current.release_version AS release_version, 
       current.taxonomy_ids as taxonomy_ids, 
       current.count AS count, 
       coalesce(current.count-previous.count, 0) as new 
FROM count_per_assembly current
LEFT JOIN count_per_assembly previous
ON current.release_version=previous.release_version+1 AND current.assembly_accession=previous.assembly_accession AND current.rs_type=previous.rs_type;
"""


def create_views_from_sql(engine):
    with engine.begin() as conn:
        conn.execute(text(create_view_taxonomy))
    with engine.begin() as conn:
        conn.execute(text(create_view_assembly))


class RSCountCategory(Base):
    """
    Table that provide the metadata associated with a number of RS id
    """
    __tablename__ = 'release_rs_count_category'

    count_category_id = Column(Integer, primary_key=True, autoincrement=True)
    assembly_accession = Column(String)
    taxonomy_id = Column(Integer)
    rs_type = Column(String)
    release_version = Column(Integer)
    rs_count_id = mapped_column(ForeignKey("release_rs_count.rs_count_id"))
    rs_count = relationship("RSCount", back_populates="count_categories")
    __table_args__ = (
        UniqueConstraint('assembly_accession', 'taxonomy_id', 'rs_type', 'release_version', 'rs_count_id', name='uix_1'),
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



