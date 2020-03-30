/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.core.batch.io;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;

import java.util.List;

public class DbsnpClusteredVariantWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    private MongoTemplate mongoTemplate;

    private ImportCounts importCounts;

    public DbsnpClusteredVariantWriter(MongoTemplate mongoTemplate, ImportCounts importCounts) {
        this.mongoTemplate = mongoTemplate;
        this.importCounts = importCounts;
    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> importedClusteredVariants) {
        try {
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                  DbsnpClusteredVariantEntity.class);
            bulkOperations.insert(importedClusteredVariants);
            bulkOperations.execute();
            importCounts.addClusteredVariantsWritten(importedClusteredVariants.size());
        } catch (DuplicateKeyException exception) {
            BulkWriteResult bulkWriteResult = ((MongoBulkWriteException) exception.getCause()).getWriteResult();
            importCounts.addClusteredVariantsWritten(bulkWriteResult.getInsertedCount());
            throw exception;
        }
    }
}
