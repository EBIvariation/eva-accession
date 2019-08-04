/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp2.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.eva.accession.core.io.DbsnpClusteredVariantWriter;
import uk.ac.ebi.eva.accession.core.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static uk.ac.ebi.eva.accession.core.utils.BulkOperationExceptionUtils.extractUniqueHashesForDuplicateKeyError;

/**
 * Writes a clustered variant to mongo DB collection: dbsnpClusteredVariantEntity
 * The writer skips the duplicates, and lists those in logger error messages for reporting
 */
public class DbsnpJsonClusteredVariantsWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpJsonClusteredVariantsWriter.class);

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private MongoTemplate mongoTemplate;

    public DbsnpJsonClusteredVariantsWriter(MongoTemplate mongoTemplate, ImportCounts importCounts) {
        dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> clusteredVariants) {
        try {
            dbsnpClusteredVariantWriter.write(clusteredVariants);
        } catch (BulkOperationException exception) {
            List<String> ids = extractUniqueHashesForDuplicateKeyError(exception).collect(toList());
            Query query = new Query();
            query.addCriteria(Criteria.where("_id").in(ids));
            List<DbsnpClusteredVariantEntity> entities = mongoTemplate.find(query, DbsnpClusteredVariantEntity.class);
            List<Long> rsIDs = entities.stream()
                .map(DbsnpClusteredVariantEntity::getAccession)
                .collect(toList());
            logger.error("Duplicate RS IDs: {}", rsIDs);
            logger.debug("Error trace", exception);
        }
    }
}
