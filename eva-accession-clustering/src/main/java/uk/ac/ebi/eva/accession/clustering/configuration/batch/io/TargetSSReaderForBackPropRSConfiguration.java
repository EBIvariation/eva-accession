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

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReader;
import uk.ac.ebi.eva.accession.clustering.batch.io.TargetSSReaderForSplitOrMergedBackPropRS;
import uk.ac.ebi.eva.accession.clustering.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.TARGET_SS_READER_FOR_NEW_BACKPROP_RS;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.TARGET_SS_READER_FOR_SPLIT_OR_MERGED_BACKPROP_RS;

@Configuration
@Import({MongoConfiguration.class, InputParametersConfiguration.class})
public class TargetSSReaderForBackPropRSConfiguration {

    @Bean(TARGET_SS_READER_FOR_NEW_BACKPROP_RS)
    public ClusteringMongoReader targetSSReaderForNewBackPropRS(MongoTemplate mongoTemplate,
                                                                 InputParameters parameters) {
        String remappedFromAssembly = parameters.getRemappedFrom();
        if (remappedFromAssembly == null) {
            throw new IllegalArgumentException("Assembly remappedFrom attribute must be provided!");
        }
        return new ClusteringMongoReader(mongoTemplate, remappedFromAssembly, parameters.getChunkSize(), false);
    }

    @Bean(TARGET_SS_READER_FOR_SPLIT_OR_MERGED_BACKPROP_RS)
    public TargetSSReaderForSplitOrMergedBackPropRS targetSSReaderForSplitOrMergedBackPropRS
            (MongoTemplate mongoTemplate, ClusteredVariantAccessioningService clusteredVariantAccessioningService,
             SubmittedVariantAccessioningService submittedVariantAccessioningService, InputParameters parameters) {
        return new TargetSSReaderForSplitOrMergedBackPropRS(mongoTemplate, parameters.getAssemblyAccession(),
                parameters.getRemappedFrom(), clusteredVariantAccessioningService, submittedVariantAccessioningService,
                parameters.getChunkSize());
    }
}
