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
package uk.ac.ebi.eva.accession.clustering.batch.io.clustering_writer;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;

import static org.junit.Assert.assertEquals;

public class ClusteringAssertions {

    /**
     * Clustering counts are used by the listener {@link ClusteringProgressListener} to summarize the counts after
     * the step is finished
     */
    static void assertClusteringCounts(ClusteringCounts clusteringCounts,
                                        long expectedClusteredVariantsCreated,
                                        long expectedClusteredVariantsUpdated,
                                        long expectedClusteredVariantsMergeOperationsWritten,
                                        long expectedSubmittedVariantsKeptUnclustered,
                                        long expectedSubmittedVariantsNewRs,
                                        long expectedSubmittedVariantsUpdatedRs,
                                        long expectedSubmittedVariantsUpdateOperationWritten) {
        assertEquals(expectedClusteredVariantsCreated, clusteringCounts.getClusteredVariantsCreated());
        assertEquals(expectedClusteredVariantsUpdated, clusteringCounts.getClusteredVariantsUpdated());
        assertEquals(expectedClusteredVariantsMergeOperationsWritten,
                clusteringCounts.getClusteredVariantsMergeOperationsWritten());
        assertEquals(expectedSubmittedVariantsKeptUnclustered, clusteringCounts.getSubmittedVariantsKeptUnclustered());
        assertEquals(expectedSubmittedVariantsNewRs, clusteringCounts.getSubmittedVariantsClustered());
        assertEquals(expectedSubmittedVariantsUpdatedRs, clusteringCounts.getSubmittedVariantsUpdatedRs());
        assertEquals(expectedSubmittedVariantsUpdateOperationWritten,
                clusteringCounts.getSubmittedVariantsUpdateOperationWritten());
    }
}
