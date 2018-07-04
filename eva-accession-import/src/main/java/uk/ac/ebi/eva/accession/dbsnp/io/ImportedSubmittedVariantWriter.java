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
package uk.ac.ebi.eva.accession.dbsnp.io;

import com.mongodb.BulkWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.dbsnp.persistence.ImportedSubmittedVariantEntity;

import java.util.List;

public class ImportedSubmittedVariantWriter implements ItemWriter<ImportedSubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ImportedSubmittedVariantWriter.class);

    private MongoTemplate mongoTemplate;

    public ImportedSubmittedVariantWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends ImportedSubmittedVariantEntity> importedSubmittedVariants) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              ImportedSubmittedVariantEntity.class);
        bulkOperations.insert(importedSubmittedVariants);
        bulkOperations.execute();
    }

}
