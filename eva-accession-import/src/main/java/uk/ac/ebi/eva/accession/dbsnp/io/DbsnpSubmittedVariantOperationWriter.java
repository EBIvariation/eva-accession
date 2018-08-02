package uk.ac.ebi.eva.accession.dbsnp.io;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;

import java.util.List;

public class DbsnpSubmittedVariantOperationWriter implements ItemWriter<DbsnpSubmittedVariantOperationEntity> {

    @Autowired
    private MongoTemplate mongoTemplate;

    public DbsnpSubmittedVariantOperationWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends DbsnpSubmittedVariantOperationEntity> importedSubmittedVariantsOperations)
            throws Exception {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              DbsnpSubmittedVariantOperationEntity.class);
        bulkOperations.insert(importedSubmittedVariantsOperations);
        bulkOperations.execute();
    }
}
