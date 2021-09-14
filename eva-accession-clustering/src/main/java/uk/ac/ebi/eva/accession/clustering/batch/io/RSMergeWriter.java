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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.beans.factory.annotation.Qualifier;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;

public class RSMergeWriter implements ItemWriter<SubmittedVariantOperationEntity> {

    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriter;

    public RSMergeWriter() {

    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantOperationEntity> submittedVariantOperationEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        for (SubmittedVariantOperationEntity entity: submittedVariantOperationEntities) {
            writeRSMerge(entity);
        }
    }

    public void writeRSMerge(SubmittedVariantOperationEntity submittedVariantOperationEntity) {
        List<ClusteredVariantEntity> mergeCandidates =
                submittedVariantOperationEntity.getInactiveObjects()
                                               .stream()
                                               .map(entity -> clusteringWriter.toClusteredVariantEntity(
                                                       toSubmittedVariantEntity(entity)))
                                               .collect(Collectors.toList());
        ImmutablePair<ClusteredVariantEntity, List<ClusteredVariantEntity>> mergeDestinationAndMergees =
                getMergeDestinationAndMergees(mergeCandidates);
        ClusteredVariantEntity mergeDestination = mergeDestinationAndMergees.getLeft();
        List<ClusteredVariantEntity> mergees = mergeDestinationAndMergees.getRight();
        mergees.forEach(mergee -> clusteringWriter.merge(mergeDestination.getAccession(),
                                                         mergeDestination.getHashedMessage(),
                                                         mergee.getAccession()));
    }

    private ImmutablePair<ClusteredVariantEntity, List<ClusteredVariantEntity>> getMergeDestinationAndMergees(List<? extends ClusteredVariantEntity> mergeCandidates) {
        Long lastPrioritizedAccession = mergeCandidates.get(0).getAccession();
        //Use the current RS prioritization policy to get the target RS into which other RS will be merged
        for (int i = 1; i < mergeCandidates.size(); i++) {
            lastPrioritizedAccession = ClusteredVariantMergingPolicy.prioritise(
                    lastPrioritizedAccession, mergeCandidates.get(i).getAccession()).accessionToKeep;
        }
        final Long targetRSAccession = lastPrioritizedAccession;
        ClusteredVariantEntity targetRS = mergeCandidates.stream().filter(rs -> rs.getAccession()
                                                                                  .equals(targetRSAccession))
                                                         .findFirst().get();
        List<ClusteredVariantEntity> mergees = mergeCandidates.stream()
                                                              .filter(rs -> !rs.getAccession()
                                                                               .equals(targetRSAccession))
                                                              .collect(Collectors.toList());
        return new ImmutablePair<>(targetRS, mergees);
    }

    private SubmittedVariantEntity toSubmittedVariantEntity(SubmittedVariantInactiveEntity
                                                                    submittedVariantInactiveEntity) {
        return new SubmittedVariantEntity(submittedVariantInactiveEntity.getAccession(),
                                          submittedVariantInactiveEntity.getHashedMessage(),
                                          submittedVariantInactiveEntity.getModel(),
                                          submittedVariantInactiveEntity.getVersion());
    }
}
