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

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantMonotonicAccessioningService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_WRITER;

@Configuration
@Import({ClusteredVariantAccessioningConfiguration.class, MongoConfiguration.class})
public class ClusteringWriterConfiguration {

    @Bean(CLUSTERING_WRITER)
    public ClusteringWriter clusteringWriter(MongoTemplate mongoTemplate, InputParameters inputParameters,
                                             ClusteredVariantMonotonicAccessioningService accessioningService) {
        return new ClusteringWriter(inputParameters.getAssemblyAccession(), mongoTemplate,
                                    accessioningService);
    }
}
