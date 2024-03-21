import sqlalchemy
from sqlalchemy import MetaData, Column, Integer, String, ForeignKey, UniqueConstraint, BigInteger, TEXT, create_engine, \
    URL, text, schema, ARRAY
from sqlalchemy.orm import declarative_base, mapped_column, relationship


metadata = MetaData(schema="eva_stats")
Base = declarative_base(metadata=metadata)


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


class RSCountPerTaxonomy(Base):
    """
    Table that provide the aggregated count per taxonomy
    """
    __tablename__ = 'release_rs_count_per_taxonomy'

    taxonomy_id = Column(Integer, primary_key=True)
    rs_type = Column(String, primary_key=True)
    release_version = Column(Integer, primary_key=True)
    release_folder = Column(String)
    assembly_accessions = Column(ARRAY(String))
    count = Column(BigInteger)
    new = Column(BigInteger)
    schema = 'eva_stats'


class RSCountPerAssembly(Base):
    """
    Table that provide the aggregated count per assembly
    """
    __tablename__ = 'release_rs_count_per_assembly'

    assembly_accession = Column(String, primary_key=True)
    rs_type = Column(String, primary_key=True)
    release_version = Column(Integer, primary_key=True)
    release_folder = Column(String)
    taxonomy_ids = Column(ARRAY(Integer))
    count = Column(BigInteger)
    new = Column(BigInteger)
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
    RSCountPerAssembly.__table__.create(bind=engine, checkfirst=True)
    RSCountPerTaxonomy.__table__.create(bind=engine, checkfirst=True)
    return engine



