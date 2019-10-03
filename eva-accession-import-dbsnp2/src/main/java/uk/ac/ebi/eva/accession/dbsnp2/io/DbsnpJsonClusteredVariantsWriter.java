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
import uk.ac.ebi.eva.accession.core.io.DbsnpClusteredVariantWriter;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static uk.ac.ebi.eva.accession.core.utils.BulkOperationExceptionUtils.extractUniqueHashesForDuplicateKeyError;

/**
 * Writes a clustered variant to mongo DB collection: dbsnpClusteredVariantEntity
 * The writer skips the duplicates, and lists those in logger error messages for reporting
 */
public class DbsnpJsonClusteredVariantsWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpJsonClusteredVariantsWriter.class);

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    public DbsnpJsonClusteredVariantsWriter(DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter) {
        this.dbsnpClusteredVariantWriter = dbsnpClusteredVariantWriter;
    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> clusteredVariants) {
        try {
            if (!clusteredVariants.isEmpty()) {
                dbsnpClusteredVariantWriter.write(clusteredVariants);
            }
            else {
                logger.warn("Could not find any clustered variants to write in the current chunk!");
            }
        } catch (BulkOperationException exception) {
            Set<String> hashes = extractUniqueHashesForDuplicateKeyError(exception).collect(toSet());
            List<String> variantsThatFailedInsert = clusteredVariants.stream()
                                                                     .filter(v -> hashes.contains(v.getHashedMessage()))
                                                                     .map(v -> v.getAccession().toString())
                                                                     .collect(toList());
            logger.error("Duplicate RS IDs: {}", variantsThatFailedInsert);
            logger.debug("Error trace", exception);
        }
    }
}
