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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoBulkWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.core.exceptions.MongoBulkWriteExceptionUtils.extractUniqueHashesForDuplicateKeyError;

public class BackPropagatedRSWriter implements ItemWriter<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(BackPropagatedRSWriter.class);

    private final String originalAssembly;

    private final String remappedAssembly;

    private final ClusteringWriter clusteringWriter;

    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    private static final String ID_ATTRIBUTE = "_id";

    private static final String RS_ATTRIBUTE = "rs";

    private final MongoTemplate mongoTemplate;

    private final ClusteringCounts clusteringCounts;

    public BackPropagatedRSWriter(String originalAssembly, String remappedAssembly, ClusteringWriter clusteringWriter,
                                  SubmittedVariantAccessioningService submittedVariantAccessioningService,
                                  MongoTemplate mongoTemplate,
                                  ClusteringCounts clusteringCounts) {
        this.originalAssembly = originalAssembly;
        this.remappedAssembly = remappedAssembly;
        this.clusteringWriter = clusteringWriter;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.mongoTemplate = mongoTemplate;
        this.clusteringCounts = clusteringCounts;
    }

    @Override
    public void write(
            @Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntitiesInOriginalAssemblyWithNoRS)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        List<Long> ssIDsToLookupInRemappedAssembly =
                submittedVariantEntitiesInOriginalAssemblyWithNoRS
                        .stream()
                        // Some dbSNP imported variants might have been explicitly de-clustered
                        // because the REF/ALT allele data provided by dbSNP was internally inconsistent
                        // and therefore the clustering was deemed unreliable.
                        // See ss68617665 in GCA_000181335.3 for example.
                        // Don't bring such variants in for clustering again.
                        .filter(SubmittedVariantEntity::isAllelesMatch)
                        .map(AccessionedDocument::getAccession).collect(Collectors.toList());
        Map<Long, List<SubmittedVariantEntity>> ssInRemappedAssemblyGroupedByID =
                submittedVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(remappedAssembly, ssIDsToLookupInRemappedAssembly)
                        .stream()
                        .map(entity -> new SubmittedVariantEntity(entity.getAccession(), entity.getHash(),
                                                                  entity.getData(),
                                                                  entity.getVersion()))
                        .filter(entity -> Objects.nonNull(entity.getClusteredVariantAccession()))
                        .collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession));
        backPropagateRS(submittedVariantEntitiesInOriginalAssemblyWithNoRS, ssInRemappedAssemblyGroupedByID);
    }

    /*
      @see <a href="https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=717877299"/>
     */
    private void backPropagateRS(
            @Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntitiesInOriginalAssemblyWithNoRS,
            Map<Long, List<SubmittedVariantEntity>> ssInRemappedAssemblyGroupedByID) {

        // Inserts to create RS record for back-propagated RS in the original assembly
        BulkOperations evaCVEInserts = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                             ClusteredVariantEntity.class);
        BulkOperations dbsnpCVEInserts = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                               DbsnpClusteredVariantEntity.class);

        // Updates to Submitted Variant Entity (SVE) collection with the RS created above
        BulkOperations evaSVEUpdates = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                             SubmittedVariantEntity.class);
        BulkOperations dbsnpSVEUpdates = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                               DbsnpSubmittedVariantEntity.class);

        // Inserts to Submitted Variant Operation Entity (SVOE) collection recording the RS updates made above
        BulkOperations evaSVOEInserts = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantOperationEntity.class);
        BulkOperations dbsnpSVOEInserts = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                DbsnpSubmittedVariantOperationEntity.class);


        int numEvaRSAssignments = 0;
        int numDbsnpRSAssignments = 0;
        int numEVARSInserts = 0;
        int numDbsnpRSInserts = 0;

        for (SubmittedVariantEntity submittedVariantEntity: submittedVariantEntitiesInOriginalAssemblyWithNoRS) {
            Long ssIDToAssignRS = submittedVariantEntity.getAccession();
            if (ssInRemappedAssemblyGroupedByID.containsKey(ssIDToAssignRS)) {
                BulkOperations bulkSVEUpdates, bulkSVOEInserts, bulkCVEInserts;
                if (clusteringWriter.isEvaSubmittedVariant(submittedVariantEntity)) {
                    bulkSVEUpdates = evaSVEUpdates;
                    bulkSVOEInserts = evaSVOEInserts;
                    numEvaRSAssignments += 1;
                } else {
                    bulkSVEUpdates = dbsnpSVEUpdates;
                    bulkSVOEInserts = dbsnpSVOEInserts;
                    numDbsnpRSAssignments += 1;
                }
                ClusteredVariantEntity newRSRecordForOriginalAssembly =
                        getSuitableRSFromRemappedSS(ssInRemappedAssemblyGroupedByID, submittedVariantEntity);
                Long rsToAssign = newRSRecordForOriginalAssembly.getAccession();

                if (clusteringWriter.getClusteredVariantCollection(rsToAssign).equals(ClusteredVariantEntity.class)) {
                    bulkCVEInserts = evaCVEInserts;
                    numEVARSInserts += 1;
                } else {
                    bulkCVEInserts = dbsnpCVEInserts;
                    numDbsnpRSInserts += 1;
                }
                bulkCVEInserts.insert(newRSRecordForOriginalAssembly);
                clusteringCounts.addClusteredVariantsCreated(1);

                Query queryToLookUpSSHash = query(where(ID_ATTRIBUTE).is(submittedVariantEntity.getHashedMessage()));
                Update updateRSInOriginalAssembly = Update.update(RS_ATTRIBUTE, rsToAssign);
                bulkSVEUpdates.updateOne(queryToLookUpSSHash, updateRSInOriginalAssembly);
                clusteringCounts.addSubmittedVariantsClustered(1);

                bulkSVOEInserts.insert(getUpdateOperation(submittedVariantEntity, rsToAssign));
                clusteringCounts.addSubmittedVariantsUpdateOperationWritten(1);
            }
        }

        handleClusteredVariantInserts(evaCVEInserts, dbsnpCVEInserts, numEVARSInserts, numDbsnpRSInserts);

        if (numEvaRSAssignments > 0) {
            evaSVEUpdates.execute();
            evaSVOEInserts.execute();
        }

        if (numDbsnpRSAssignments > 0) {
            dbsnpSVEUpdates.execute();
            dbsnpSVOEInserts.execute();
        }
    }

    private void handleClusteredVariantInserts(BulkOperations evaCVEInserts, BulkOperations dbsnpCVEInserts,
                                               int numEVARSInserts, int numDbsnpRSInserts) {
        try {
            if (numEVARSInserts > 0) {
                evaCVEInserts.execute();
            }

            if (numDbsnpRSInserts > 0) {
                dbsnpCVEInserts.execute();
            }
        }
        catch (DuplicateKeyException duplicateKeyException) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) duplicateKeyException.getCause());
            // Consider a scenario where we are back-propagating rs ID:
            // RS1 corresponding to SS1 from ASM2 (remapped assembly) to ASM1 (original assembly)
            // If the hash for back-propagated RS1 already exists in ASM1,
            // it means that, for some reason, SS1 was not clustered with RS1 when ASM1 underwent clustering.
            // It is better to flag such issues for later analysis.
            extractUniqueHashesForDuplicateKeyError(writeException)
                    .forEach(hash -> logger.error("Attempted to insert RS record with already existing hash " + hash +
                                                         "during RS ID back-propagation! This could be symptomatic " +
                                                         "of clustering issues with the original assembly "
                                                         + this.originalAssembly + "."));
        }
    }

    private ClusteredVariantEntity getSuitableRSFromRemappedSS(
            Map<Long, List<SubmittedVariantEntity>> ssInRemappedAssemblyGroupedByID,
            SubmittedVariantEntity submittedVariantEntity) {
        // Currently if there is more than one entry for the SS accession in the remapped assembly,
        // we choose the minimum RS ID as a hack since we don't have a way to map between the
        // remapped SS and the original SS in such cases
        // TODO: Re-visit during EVA-2612
        ClusteredVariantEntity rsFromRemappedAssembly =
                ssInRemappedAssemblyGroupedByID
                        .get(submittedVariantEntity.getAccession())
                        .stream()
                        .max(Comparator.comparingLong(SubmittedVariantEntity::getClusteredVariantAccession))
                        .map(clusteringWriter::toClusteredVariantEntity).get();

        ClusteredVariantEntity newRSRecordForOriginalAssembly =
                clusteringWriter.toClusteredVariantEntity(submittedVariantEntity);
        // The above statement will only create an object with the null accession as in the original assembly
        // The following is needed to update that accession to the back-propagated RS ID from the remapped assembly
        newRSRecordForOriginalAssembly =
                new ClusteredVariantEntity(rsFromRemappedAssembly.getAccession(),
                                           newRSRecordForOriginalAssembly.getHashedMessage(),
                                           newRSRecordForOriginalAssembly.getModel(),
                                           newRSRecordForOriginalAssembly.getVersion());
        return newRSRecordForOriginalAssembly;
    }

    private SubmittedVariantOperationEntity getUpdateOperation(SubmittedVariantEntity submittedVariantEntity,
                                                               Long assignedRS) {
        // Query to create the update operation history
        SubmittedVariantOperationEntity updateOperation = new SubmittedVariantOperationEntity();
        SubmittedVariantInactiveEntity inactiveEntity = new SubmittedVariantInactiveEntity(submittedVariantEntity);
        updateOperation.fill(
                EventType.UPDATED,
                submittedVariantEntity.getAccession(),
                null,
                "Back-propagating rs" + assignedRS + " for submitted variant ss" +
                        submittedVariantEntity.getAccession() + " after remapping to " + this.remappedAssembly + ".",
                Collections.singletonList(inactiveEntity)
        );
        return updateOperation;
    }
}
