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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReader;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReaderRetryable;
import uk.ac.ebi.eva.accession.clustering.batch.io.StudyClusteringMongoReader;
import uk.ac.ebi.eva.accession.clustering.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_VARIANTS_MONGO_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_VARIANTS_MONGO_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.STUDY_CLUSTERING_MONGO_READER;

@Configuration
@Import({MongoConfiguration.class, InputParametersConfiguration.class})
public class ClusteringMongoReaderConfiguration {

    @Bean(CLUSTERED_VARIANTS_MONGO_READER)
    @StepScope
    public ClusteringMongoReader clusteredVariantsMongoReader(MongoTemplate mongoTemplate, InputParameters parameters) {
        if (parameters.getAssemblyAccession() == null || parameters.getAssemblyAccession().isEmpty()) {
            throw new IllegalArgumentException("Please provide an assembly");
        }

        if (parameters.isAllowRetry()) {
            return new ClusteringMongoReaderRetryable(mongoTemplate, parameters.getAssemblyAccession(),
                                                      parameters.getChunkSize(), true);
        } else {
            return new ClusteringMongoReader(mongoTemplate, parameters.getAssemblyAccession(), parameters.getChunkSize(),
                                             true);
        }
    }

    @Bean(NON_CLUSTERED_VARIANTS_MONGO_READER)
    @StepScope
    public ClusteringMongoReader nonClusteredVariantsMongoReader(MongoTemplate mongoTemplate,
                                                                 InputParameters parameters) {
        if (parameters.getAssemblyAccession() == null || parameters.getAssemblyAccession().isEmpty()) {
            throw new IllegalArgumentException("Please provide an assembly");
        }

        if (parameters.isAllowRetry()) {
            return new ClusteringMongoReaderRetryable(mongoTemplate, parameters.getAssemblyAccession(),
                                                      parameters.getChunkSize(), false);
        } else {
            return new ClusteringMongoReader(mongoTemplate, parameters.getAssemblyAccession(), parameters.getChunkSize(),
                                             false);
        }
    }

    @Bean(STUDY_CLUSTERING_MONGO_READER)
    @StepScope
    public StudyClusteringMongoReader studyClusteringMongoReader(MongoTemplate mongoTemplate,
                                                                 InputParameters parameters) {
        if (parameters.getAssemblyAccession() == null || parameters.getAssemblyAccession().isEmpty()) {
            throw new IllegalArgumentException("Please provide an assembly");
        }
        if (parameters.getProjects() == null || parameters.getProjects().isEmpty()) {
            throw new IllegalArgumentException("Please provide one or more studies");
        }
        return new StudyClusteringMongoReader(mongoTemplate, parameters.getAssemblyAccession(),
                                              parameters.getProjects(), parameters.getChunkSize());
    }
}
