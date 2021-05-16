/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.batch.io;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.exceptions.MongoBulkWriteExceptionUtils;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.remapping.ingest.batch.listeners.RemappingIngestCounts;
import uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames;

import java.util.List;

public class RemappedSubmittedVariantsWriter implements ItemWriter<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(RemappedSubmittedVariantsWriter.class);

    private MongoTemplate mongoTemplate;

    private String collection;

    private RemappingIngestCounts remappingIngestCounts;

    public RemappedSubmittedVariantsWriter(MongoTemplate mongoTemplate, String collection,
                                           RemappingIngestCounts remappingIngestCounts) {
        this.mongoTemplate = mongoTemplate;
        this.collection = collection;
        this.remappingIngestCounts = remappingIngestCounts;
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> submittedVariantsRemapped) {
        try {
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                  SubmittedVariantEntity.class,
                                                                  collection);
            bulkOperations.insert(submittedVariantsRemapped);
            BulkWriteResult bulkWriteResult = bulkOperations.execute();
            remappingIngestCounts.addRemappedVariantsIngested(bulkWriteResult.getInsertedCount());
        } catch (DuplicateKeyException exception) {
            logger.warn(exception.toString());

            MongoBulkWriteException writeException = ((MongoBulkWriteException) exception.getCause());
            BulkWriteResult bulkWriteResult = writeException.getWriteResult();

            int ingested = bulkWriteResult.getInsertedCount();
            remappingIngestCounts.addRemappedVariantsIngested(ingested);

            long duplicatesSkipped = MongoBulkWriteExceptionUtils
                    .extractUniqueHashesForDuplicateKeyError(writeException).count();
            remappingIngestCounts.addRemappedVariantsSkipped(duplicatesSkipped);
        }
    }
}
