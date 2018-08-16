package uk.ac.ebi.eva.accession.dbsnp.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;

import java.util.List;

public class DbsnpClusteredVariantDeclusteredWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpClusteredVariantWriter.class);

    static final String DBSNP_CLUSTERED_VARIANT_DECLUSTERED = "dbsnpClusteredVariantEntityDeclustered";

    private MongoTemplate mongoTemplate;

    public DbsnpClusteredVariantDeclusteredWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> importedClusteredVariants) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              DbsnpClusteredVariantEntity.class,
                                                              DBSNP_CLUSTERED_VARIANT_DECLUSTERED);
        bulkOperations.insert(importedClusteredVariants);
        bulkOperations.execute();
    }

}
