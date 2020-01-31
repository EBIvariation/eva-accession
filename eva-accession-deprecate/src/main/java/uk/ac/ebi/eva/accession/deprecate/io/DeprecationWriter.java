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
package uk.ac.ebi.eva.accession.deprecate.io;

import com.mongodb.BulkWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class DeprecationWriter implements ItemWriter<DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DeprecationWriter.class);

    private static final String DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME =
            "dbsnpClusteredVariantEntityDeclustered";

    private static final String ID_FIELD = "_id";

    private static final String ACCESSION_FIELD = "accession";

    private MongoTemplate mongoTemplate;

    public DeprecationWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVariants) throws Exception {
        try {
            insertDeprecateOperation(deprecableClusteredVariants);
            removeDeprecableClusteredVariantsDeprecated(deprecableClusteredVariants);
            removeDeprecableClusteredVariants(deprecableClusteredVariants);
        } catch (BulkOperationException e) {
            BulkWriteResult bulkWriteResult = e.getResult();
            logger.error("Deprecation writer failed. chunk size: {}, written operations: {}, removed rs ids: {}",
                         deprecableClusteredVariants.size(), bulkWriteResult.getInsertedCount(),
                         bulkWriteResult.getRemovedCount());
            logger.error(
                    "RS IDs present in this failed chunk: [" + getAccessionsString(deprecableClusteredVariants) + "]");
            throw e;
        }
    }

    private String getAccessionsString(List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVariants) {
        return deprecableClusteredVariants.stream()
                                          .map(AccessionedDocument::getAccession)
                                          .map(Objects::toString)
                                          .collect(Collectors.joining(", "));
    }

    /**
     * Note that we remove documents by id. The reason is that we are iterating over the same collection, and it's not
     * a good idea to remove elements that you haven't read yet (and there could be elements with the same accession
     * and different id (hash) that we haven't read yet).
     */
    private void removeDeprecableClusteredVariantsDeprecated(
            List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVariants) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              DbsnpClusteredVariantEntity.class,
                                                              DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        Query query = new Query();
        query.addCriteria(Criteria.where(ID_FIELD).in(getIds(deprecableClusteredVariants)));
        bulkOperations.remove(query);
        bulkOperations.execute();
    }

    private List<String> getIds(List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVariants) {
        return deprecableClusteredVariants.stream().map(AccessionedDocument::getId).collect(Collectors.toList());
    }

    /**
     * Note that we remove ClusteredVariants by accession. Unlike the removal from the
     * {@link DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME} collection, here we don't care if we remove all the
     * documents for the given accession, even if there are several hashes for it, because the
     * {@link DeprecableClusteredVariantsReader} said that no SubmittedVariant is associated to the accession (whatever
     * hashes the accession might have).
     *
     *
     * Also, note that we can't remove by only the id (the hash) because other unrelated and active ClusteredVariants
     * accessions would be removed erroneously. If we wanted to remove less ClusteredVariants, we could remove by id
     * (hash) and accession, but that is problematic as well because it could leave as "active variants" some
     * ClusteredVariants without SubmittedVariants, depending on whether the removed accession is also active with the
     * same or a different hash. Look at this example of the resulting collections after an import:
     *
     * <pre>
     * Declustered          Clustered             Submitted                 SubmittedOperations
     * -                    RS1 (with hash 1)     SS1 (pointing to RS1)     -
     * -                    RS2 (with hash 2)     -                         SS2 (mergedInto SS1)
     * RS2 (with hash 2)    -                     -                         SS3 (declustered)
     * RS2 (with hash 3)    -                     -                         SS4 (declustered)
     * </pre>
     *
     * If the third line was present (RS2 with hash 2 and SS3), then the active RS2 (second line) would be removed.
     * However, if only the fourth line was present (RS2 with hash 3 and SS4), the active RS2 (second line) wouldn't be
     * removed. Examples like these suggest that we should only remove ClusteredVariants by accession, not using the id
     * (the hash) at all.
     */
    private void removeDeprecableClusteredVariants(
            List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVariants) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              DbsnpClusteredVariantEntity.class);
        Query query = new Query();
        query.addCriteria(Criteria.where(ACCESSION_FIELD).in(getAccessions(deprecableClusteredVariants)));
        bulkOperations.remove(query);
        bulkOperations.execute();
    }

    private List<Long> getAccessions(List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVariants) {
        return deprecableClusteredVariants.stream().map(AccessionedDocument::getAccession).collect(Collectors.toList());
    }

    private void insertDeprecateOperation(List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVatiants) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              DbsnpClusteredVariantOperationEntity.class);
        bulkOperations.insert(createOperations(deprecableClusteredVatiants));
        bulkOperations.execute();
    }

    private List<DbsnpClusteredVariantOperationEntity> createOperations(
            List<? extends DbsnpClusteredVariantEntity> deprecableClusteredVatiants) {
        List<DbsnpClusteredVariantOperationEntity> operations = new ArrayList<>();
        for (DbsnpClusteredVariantEntity deprecableClusteredVatiant : deprecableClusteredVatiants) {
            DbsnpClusteredVariantInactiveEntity inactiveEntity = new DbsnpClusteredVariantInactiveEntity(
                    deprecableClusteredVatiant);
            DbsnpClusteredVariantOperationEntity operation = new DbsnpClusteredVariantOperationEntity();
            operation.fill(EventType.DEPRECATED, deprecableClusteredVatiant.getAccession(), null,
                           "Clustered variant completely declustered", Collections.singletonList(inactiveEntity));
            operations.add(operation);
        }
        return operations;
    }
}