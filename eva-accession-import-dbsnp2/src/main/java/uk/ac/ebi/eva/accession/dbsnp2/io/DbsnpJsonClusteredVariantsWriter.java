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
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.batch.io.DbsnpClusteredVariantWriter;
import uk.ac.ebi.eva.accession.core.batch.io.DbsnpClusteredVariantOperationWriter;
import uk.ac.ebi.eva.accession.core.batch.io.MergeOperationBuilder;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantOperationRepository;

import java.util.Arrays;
import java.util.List;

/**
 * Writes a clustered variant to mongo DB collection: dbsnpClusteredVariantEntity
 * The writer skips the duplicates, and lists those in logger error messages for reporting
 */
public class DbsnpJsonClusteredVariantsWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpJsonClusteredVariantsWriter.class);

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter;

    private MergeOperationBuilder<DbsnpClusteredVariantEntity, DbsnpClusteredVariantOperationEntity>
            clusteredOperationBuilder;

    public DbsnpJsonClusteredVariantsWriter(DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter,
                                            DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter,
                                            DbsnpClusteredVariantOperationRepository clusteredOperationRepository,
                                            DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository) {
        this.dbsnpClusteredVariantWriter = dbsnpClusteredVariantWriter;
        this.dbsnpClusteredVariantOperationWriter = dbsnpClusteredVariantOperationWriter;
        this.clusteredOperationBuilder = new MergeOperationBuilder<>(
                clusteredOperationRepository, clusteredVariantRepository, this::buildClusteredMergeOperation);

    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> clusteredVariants) throws Exception {
        try {
            if (!clusteredVariants.isEmpty()) {
                dbsnpClusteredVariantWriter.write(clusteredVariants);
            }
            else {
                logger.warn("Could not find any clustered variants to write in the current chunk!");
            }
        } catch (BulkOperationException exception) {
            List<DbsnpClusteredVariantOperationEntity> mergeClusteredOperations =
                    clusteredOperationBuilder.buildMergeOperationsFromException(
                            (List<DbsnpClusteredVariantEntity>) clusteredVariants, exception);
            if (!mergeClusteredOperations.isEmpty()) {
                dbsnpClusteredVariantOperationWriter.write(mergeClusteredOperations);
            }
        }
    }

    private DbsnpClusteredVariantOperationEntity buildClusteredMergeOperation(DbsnpClusteredVariantEntity origin,
                                                                              DbsnpClusteredVariantEntity mergedInto) {
        DbsnpClusteredVariantInactiveEntity inactiveEntity = new DbsnpClusteredVariantInactiveEntity(origin);

        DbsnpClusteredVariantOperationEntity operation = new DbsnpClusteredVariantOperationEntity();
        operation.fill(EventType.MERGED, origin.getAccession(), mergedInto.getAccession(),
                       "Identical clustered variant received multiple RS identifiers",
                       Arrays.asList(inactiveEntity));

        return operation;
    }
}
