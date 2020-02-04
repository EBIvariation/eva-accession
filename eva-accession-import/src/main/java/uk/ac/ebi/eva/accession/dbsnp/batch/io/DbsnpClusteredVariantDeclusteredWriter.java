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
package uk.ac.ebi.eva.accession.dbsnp.batch.io;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;

import java.util.List;

/**
 * Writes into a separate collection those clustered variants (RS) for which at least one submitted variant (SS) has
 * been declustered.
 */
public class DbsnpClusteredVariantDeclusteredWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    public static final String DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME =
            "dbsnpClusteredVariantEntityDeclustered";

    private MongoTemplate mongoTemplate;

    public DbsnpClusteredVariantDeclusteredWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> importedClusteredVariants) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              DbsnpClusteredVariantEntity.class,
                                                              DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        bulkOperations.insert(importedClusteredVariants);

        // Let exceptions be raised. Ignore duplicate key exceptions in user code only if the accession is the same.
        bulkOperations.execute();

    }
}
