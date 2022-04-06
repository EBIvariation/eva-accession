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

import com.mongodb.MongoBulkWriteException;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class BackPropagatedRSWriter implements ItemWriter<SubmittedVariantEntity> {

    private final String remappedAssembly;

    private final ClusteringWriter clusteringWriter;

    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    private static final String ID_ATTRIBUTE = "_id";

    private static final String BACKPROP_RS_ATTRIBUTE = "backPropRS";

    private final MongoTemplate mongoTemplate;

    private final MetricCompute metricCompute;

    public BackPropagatedRSWriter(String remappedAssembly, ClusteringWriter clusteringWriter,
                                  SubmittedVariantAccessioningService submittedVariantAccessioningService,
                                  MongoTemplate mongoTemplate, MetricCompute metricCompute) {
        this.remappedAssembly = remappedAssembly;
        this.clusteringWriter = clusteringWriter;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.mongoTemplate = mongoTemplate;
        this.metricCompute = metricCompute;
    }

    @Override
    public void write(
            @Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntitiesInOriginalAssemblyWithNoRS)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        List<SubmittedVariantEntity> ssToLookupInRemappedAssembly =
                submittedVariantEntitiesInOriginalAssemblyWithNoRS
                        .stream()
                        // Some dbSNP imported variants might have been explicitly de-clustered
                        // because the REF/ALT allele data provided by dbSNP was internally inconsistent
                        // and therefore the clustering was deemed unreliable.
                        // See ss68617665 in GCA_000181335.3 for example.
                        // Don't bring such variants in for clustering again.
                        .filter(SubmittedVariantEntity::isAllelesMatch)
                        // Ensure that we don't backpropagate to SS IDs that already have backpropagated RS
                        .filter(ss -> Objects.isNull(ss.getBackPropagatedVariantAccession()))
                        .collect(Collectors.toList());
        List<Long> ssIDsToLookupInRemappedAssembly =
                ssToLookupInRemappedAssembly.stream().map(AccessionedDocument::getAccession)
                                            .collect(Collectors.toList());

        Map<Long, List<SubmittedVariantEntity>> ssInRemappedAssemblyGroupedByID =
                submittedVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(remappedAssembly, ssIDsToLookupInRemappedAssembly)
                        .stream()
                        .map(entity -> new SubmittedVariantEntity(entity.getAccession(), entity.getHash(),
                                                                  entity.getData(),
                                                                  entity.getVersion()))
                        .filter(entity -> Objects.nonNull(entity.getClusteredVariantAccession()))
                        .collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession));
        assignRSToSS(ssToLookupInRemappedAssembly, ssInRemappedAssemblyGroupedByID);
    }

    private void assignRSToSS(
            @Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntitiesInOriginalAssemblyWithNoRS,
            Map<Long, List<SubmittedVariantEntity>> ssInRemappedAssemblyGroupedByID) {

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

        for (SubmittedVariantEntity submittedVariantEntity: submittedVariantEntitiesInOriginalAssemblyWithNoRS) {
            Long ssIDToBeClustered = submittedVariantEntity.getAccession();
            Long rsToBackPropagate = null;

            // Back-propagate RS from the remapped assembly
            if (ssInRemappedAssemblyGroupedByID.containsKey(ssIDToBeClustered)) {
                rsToBackPropagate =
                        getSuitableRSFromRemappedSS(ssInRemappedAssemblyGroupedByID, submittedVariantEntity);
            }

            if (Objects.nonNull(rsToBackPropagate)) {
                BulkOperations bulkSVEUpdates, bulkSVOEInserts;
                if (clusteringWriter.isEvaSubmittedVariant(submittedVariantEntity)) {
                    bulkSVEUpdates = evaSVEUpdates;
                    bulkSVOEInserts = evaSVOEInserts;
                    numEvaRSAssignments += 1;
                } else {
                    bulkSVEUpdates = dbsnpSVEUpdates;
                    bulkSVOEInserts = dbsnpSVOEInserts;
                    numDbsnpRSAssignments += 1;
                }

                Query queryToLookUpSSHash = query(where(ID_ATTRIBUTE).is(submittedVariantEntity.getHashedMessage()));
                Update updateRSInOriginalAssembly = Update.update(BACKPROP_RS_ATTRIBUTE, rsToBackPropagate);
                bulkSVEUpdates.updateOne(queryToLookUpSSHash, updateRSInOriginalAssembly);

                bulkSVOEInserts.insert(getUpdateOperation(submittedVariantEntity, rsToBackPropagate));
                metricCompute.addCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATE_OPERATIONS, 1);
            }
        }

        if (numEvaRSAssignments > 0) {
            evaSVEUpdates.execute();
            evaSVOEInserts.execute();
        }

        if (numDbsnpRSAssignments > 0) {
            dbsnpSVEUpdates.execute();
            dbsnpSVOEInserts.execute();
        }
    }

    private Long getSuitableRSFromRemappedSS(
            Map<Long, List<SubmittedVariantEntity>> ssInRemappedAssemblyGroupedByID,
            SubmittedVariantEntity submittedVariantEntity) {
        // Currently if there is more than one entry for the SS accession in the remapped assembly,
        // we choose the maximum RS ID as a hack since we don't have a way to map between the
        // remapped SS and the original SS in such cases
        // TODO: Re-visit during EVA-2612
        ClusteredVariantEntity rsFromRemappedAssembly =
                ssInRemappedAssemblyGroupedByID
                        .get(submittedVariantEntity.getAccession())
                        .stream()
                        .max(Comparator.comparingLong(SubmittedVariantEntity::getClusteredVariantAccession))
                        .map(clusteringWriter::toClusteredVariantEntity).get();

        return rsFromRemappedAssembly.getAccession();
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
