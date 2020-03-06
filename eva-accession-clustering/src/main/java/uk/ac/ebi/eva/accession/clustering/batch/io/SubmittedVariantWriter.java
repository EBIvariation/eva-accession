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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;

import java.util.List;

public class SubmittedVariantWriter implements ItemWriter<List<SubmittedVariant>> {

    private MongoTemplate mongoTemplate;

    public SubmittedVariantWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends List<SubmittedVariant>> clusteredSubmittedVariants) throws Exception {
        try {
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                    SubmittedVariant.class);
            bulkOperations.insert(clusteredSubmittedVariants.get(0));
            bulkOperations.execute();
        } catch (BulkOperationException e) {
            throw e;
        }
    }
}
