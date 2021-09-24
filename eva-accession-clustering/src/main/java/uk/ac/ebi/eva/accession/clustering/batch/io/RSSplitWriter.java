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
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;

public class RSSplitWriter implements ItemWriter<SubmittedVariantOperationEntity> {

    private final ClusteringWriter clusteringWriter;

    private final MonotonicAccessionGenerator<IClusteredVariant> clusteredVariantAccessionGenerator;

    private final MongoTemplate mongoTemplate;

    public RSSplitWriter(ClusteringWriter clusteringWriter,
                         MonotonicAccessionGenerator<IClusteredVariant> clusteredVariantAccessionGenerator,
                         MongoTemplate mongoTemplate) {
        this.clusteringWriter = clusteringWriter;
        this.clusteredVariantAccessionGenerator = clusteredVariantAccessionGenerator;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantOperationEntity> submittedVariantOperationEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        for (SubmittedVariantOperationEntity entity: submittedVariantOperationEntities) {
            writeRSSplit(entity);
        }
    }

    public void writeRSSplit(SubmittedVariantOperationEntity submittedVariantOperationEntity)
            throws AccessionCouldNotBeGeneratedException {
        // For each sets of RS hashes among the split candidates
        // get a triple with
        // 1) ClusteredVariant object with the hash 2) the hash itself and 3) number of variants with that hash
        // ex: if the split candidates post-remapping are:
        // SS1, RS1, LOC1, SNV, C/A
        // SS2, RS1, LOC2, SNV, C/T
        // SS3, RS1, LOC2, SNV, T/G
        // SS4, RS1, LOC3, SNV, T/A
        // the result will be a list of 3 triples:
        // RS1, hash(LOC1), 1
        // RS1, hash(LOC2), 2
        // RS1, hash(LOC3), 1
        List<Triple<ClusteredVariantEntity, String, Integer>> splitCandidates =
                submittedVariantOperationEntity
                        .getInactiveObjects()
                        .stream()
                        .map(submittedVariantInactiveEntity ->
                                     clusteringWriter.toClusteredVariantEntity(
                                             submittedVariantInactiveEntity.toSubmittedVariantEntity()))
                        .collect(Collectors.groupingBy(ClusteredVariantEntity::getHashedMessage))
                        .entrySet().stream().map(hashAndCorrespondingClusteredVariants ->
                                new ImmutableTriple<>(hashAndCorrespondingClusteredVariants.getValue().get(0),
                                                      hashAndCorrespondingClusteredVariants.getKey(),
                                                      hashAndCorrespondingClusteredVariants.getValue().size()))
                        .collect(Collectors.toList());
        // Based on the split policy, one of the hashes will retain the RS associated with it
        // and the other hashes should be associated with new RS IDs
        List<String> hashesThatShouldGetNewRS =  getHashesThatShouldGetNewRS(splitCandidates);
        issueNewRSForHashes(hashesThatShouldGetNewRS, submittedVariantOperationEntity.getInactiveObjects());
    }

    private void issueNewRSForHashes(List<String> hashesThatShouldGetNewRS,
                                     List<SubmittedVariantInactiveEntity> submittedVariantInactiveEntities)
            throws AccessionCouldNotBeGeneratedException {
        for (SubmittedVariantInactiveEntity inactiveEntity: submittedVariantInactiveEntities) {
            ClusteredVariantEntity clusteredVariantEntity =
                    clusteringWriter.toClusteredVariantEntity(inactiveEntity.toSubmittedVariantEntity());
            Long oldRSAccession = clusteredVariantEntity.getAccession();
            if (hashesThatShouldGetNewRS.contains(clusteredVariantEntity.getHashedMessage())) {
                Long newRSAccession =
                        this.clusteredVariantAccessionGenerator.generateAccessions(1)[0];
                associateNewRSToSS(newRSAccession, inactiveEntity);
                writeRSUpdateOperation(oldRSAccession, newRSAccession, clusteredVariantEntity);
                writeSSUpdateOperation(oldRSAccession, newRSAccession, inactiveEntity);
            }
        }
    }

    private void associateNewRSToSS(Long newRSAccession,
                                    SubmittedVariantInactiveEntity submittedVariantInactiveEntity) {
        final String accessionAttribute = "accession";
        final String assemblyAttribute = "seq";
        final String clusteredVariantAttribute = "rs";

        SubmittedVariantEntity submittedVariantEntity = submittedVariantInactiveEntity.toSubmittedVariantEntity();
        Class<? extends SubmittedVariantEntity> submittedVariantClass =
                clusteringWriter.isEvaSubmittedVariant(submittedVariantEntity) ?
                        SubmittedVariantEntity.class : DbsnpSubmittedVariantEntity.class;
        Query queryToFindSS = query(where(assemblyAttribute).is(submittedVariantEntity.getReferenceSequenceAccession()))
                .addCriteria(where(accessionAttribute).is(submittedVariantEntity.getAccession()));
        Update updateRS = update(clusteredVariantAttribute, newRSAccession);
        this.mongoTemplate.updateMulti(queryToFindSS, updateRS, submittedVariantClass);
    }

    private void writeRSUpdateOperation(Long oldRSAccession, Long newRSAccession,
                                        ClusteredVariantEntity clusteredVariantEntity) {
        // Choose which operation collection to write to: EVA or dbSNP
        Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                operationClass = clusteringWriter.getClusteredOperationCollection(oldRSAccession);
        ClusteredVariantOperationEntity splitOperation = new ClusteredVariantOperationEntity();
        String splitOperationDescription = "Due to hash mismatch, rs" + newRSAccession +
                " was issued to split from rs" + oldRSAccession + ".";
        splitOperation.fill(EventType.RS_SPLIT, oldRSAccession, newRSAccession, splitOperationDescription,
                            Collections.singletonList(new ClusteredVariantInactiveEntity(clusteredVariantEntity)));
        this.mongoTemplate.insert(splitOperation, this.mongoTemplate.getCollectionName(operationClass));
    }

    private void writeSSUpdateOperation(Long oldRSAccession, Long newRSAccession,
                                        SubmittedVariantInactiveEntity submittedVariantInactiveEntity) {
        SubmittedVariantEntity submittedVariantEntity = submittedVariantInactiveEntity.toSubmittedVariantEntity();
        // Choose which operation collection to write to: EVA or dbSNP
        Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                operationClass = clusteringWriter.isEvaSubmittedVariant(submittedVariantEntity) ?
                SubmittedVariantOperationEntity.class : DbsnpSubmittedVariantOperationEntity.class;
        SubmittedVariantOperationEntity updateOperation = new SubmittedVariantOperationEntity();
        String updateOperationDescription = "SS was associated with the split RS rs" + newRSAccession
                + " that was split from rs" + oldRSAccession + " after remapping.";
        updateOperation.fill(EventType.UPDATED, updateOperationDescription,
                             Collections.singletonList(submittedVariantInactiveEntity));
        this.mongoTemplate.insert(updateOperation, this.mongoTemplate.getCollectionName(operationClass));
    }

    /**
     * Get hashes that should receive the new RS
     * @param splitCandidates List of triples
     *                        1) ClusteredVariant object with the hash 2) the hash itself and
     *                        3) the number of variants with that hash
     * @return Hashes that should be associated with a new RS
     */
    private List<String> getHashesThatShouldGetNewRS(List<Triple<ClusteredVariantEntity, String, Integer>>
                                                             splitCandidates) {
        Triple<ClusteredVariantEntity, String, Integer> lastPrioritizedHash = splitCandidates.get(0);
        for (int i = 1; i < splitCandidates.size(); i++) {
            lastPrioritizedHash = ClusteredVariantSplittingPolicy.prioritise(
                    lastPrioritizedHash, splitCandidates.get(i)).hashThatShouldRetainOldRS;
        }
        final Triple<ClusteredVariantEntity, String, Integer> hashThatShouldRetainOldRS = lastPrioritizedHash;
        this.mongoTemplate.save(hashThatShouldRetainOldRS.getLeft(),
                                this.mongoTemplate.getCollectionName(clusteringWriter.getClusteredVariantCollection(
                                        hashThatShouldRetainOldRS.getLeft().getAccession()))
        );
        return splitCandidates.stream()
                              .filter(candidate -> !(candidate.getMiddle().equals(
                                      hashThatShouldRetainOldRS.getMiddle())))
                              .map(Triple::getMiddle)
                              .collect(Collectors.toList());
    }
}
