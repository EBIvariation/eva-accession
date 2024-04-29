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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.RSMergeWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.RSSplitWriter;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;

@Configuration
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class,
        ClusteringWriterConfiguration.class, MongoConfiguration.class})
public class RSMergeAndSplitWriterConfiguration {

    @Autowired
    private InputParameters inputParameters;

    @Bean(RS_MERGE_WRITER)
    public ItemWriter<SubmittedVariantOperationEntity> rsMergeWriter(
            @Qualifier(CLUSTERED_CLUSTERING_WRITER) ClusteringWriter clusteringWriter,
            MongoTemplate mongoTemplate, InputParameters parameters,
            SubmittedVariantAccessioningService submittedVariantAccessioningService,
            MetricCompute metricCompute) {
        return new RSMergeWriter(clusteringWriter, mongoTemplate, parameters.getAssemblyAccession(),
                                 submittedVariantAccessioningService, metricCompute);
    }

    @Bean(RS_SPLIT_WRITER)
    public ItemWriter<SubmittedVariantOperationEntity> rsSplitWriter(
            @Qualifier(CLUSTERED_CLUSTERING_WRITER) ClusteringWriter clusteringWriter,
            ClusteredVariantAccessioningService clusteredVariantAccessioningService,
            SubmittedVariantAccessioningService submittedVariantAccessioningService,
            MongoTemplate mongoTemplate,
            MetricCompute metricCompute,
            File rsReportFile) throws IOException {
        return new RSSplitWriter(clusteringWriter, clusteredVariantAccessioningService,
                                 submittedVariantAccessioningService, mongoTemplate, metricCompute,
                                 rsReportFile);
    }
}
