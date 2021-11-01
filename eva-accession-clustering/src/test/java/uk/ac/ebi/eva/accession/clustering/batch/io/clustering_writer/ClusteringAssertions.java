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

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;
import uk.ac.ebi.eva.metrics.metric.ClusteringMetric;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static org.junit.Assert.assertEquals;

public class ClusteringAssertions {

    /**
     * Clustering counts are used by the listener {@link ClusteringProgressListener} to summarize the counts after
     * the step is finished
     */
    static void assertClusteringCounts(MetricCompute metricCompute,
                                       long expectedClusteredVariantsCreated,
                                       long expectedClusteredVariantsUpdated,
                                       long expectedClusteredVariantsMergeOperationsWritten,
                                       long expectedSubmittedVariantsKeptUnclustered,
                                       long expectedSubmittedVariantsNewRs,
                                       long expectedSubmittedVariantsUpdatedRs,
                                       long expectedSubmittedVariantsUpdateOperationWritten) {
        assertEquals(expectedClusteredVariantsCreated, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_CREATED));
        assertEquals(expectedClusteredVariantsUpdated, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_UPDATED));
        assertEquals(expectedClusteredVariantsMergeOperationsWritten,
                metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_MERGE_OPERATIONS));
        assertEquals(expectedSubmittedVariantsKeptUnclustered, ClusteringMetric.SUBMITTED_VARIANTS_KEPT_UNCLUSTERED.getCount());
        assertEquals(expectedSubmittedVariantsNewRs, ClusteringMetric.SUBMITTED_VARIANTS_CLUSTERED.getCount());
        assertEquals(expectedSubmittedVariantsUpdatedRs, ClusteringMetric.SUBMITTED_VARIANTS_UPDATED_RS.getCount());
        assertEquals(expectedSubmittedVariantsUpdateOperationWritten, ClusteringMetric.SUBMITTED_VARIANTS_UPDATE_OPERATIONS.getCount());
    }
}