/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.ingest.remapped.batch.io;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;

import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.SUBMITTED_VARIANT_ENTITY;

public class RemappedSubmittedVariantsWriter implements ItemWriter<SubmittedVariantEntity> {

    private MongoTemplate mongoTemplate;

    public RemappedSubmittedVariantsWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> submittedVariantsRemapped) throws Exception {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantEntity.class,
                                                              SUBMITTED_VARIANT_ENTITY);
        bulkOperations.insert(submittedVariantsRemapped);
        bulkOperations.execute();
    }
}
