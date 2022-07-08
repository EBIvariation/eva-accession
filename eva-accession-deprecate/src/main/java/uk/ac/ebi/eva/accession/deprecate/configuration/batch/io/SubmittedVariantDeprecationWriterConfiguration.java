/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.batch.io.SubmittedVariantDeprecationWriter;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames;
import uk.ac.ebi.eva.accession.deprecate.parameters.InputParameters;

@Configuration
@Import({MongoConfiguration.class, SubmittedVariantAccessioningConfiguration.class,
        ClusteredVariantAccessioningConfiguration.class})
public class SubmittedVariantDeprecationWriterConfiguration {

    @Bean(BeanNames.STUDY_DEPRECATION_WRITER)
    @StepScope
    SubmittedVariantDeprecationWriter deprecationWriter
            (MongoTemplate mongoTemplate, InputParameters parameters,
             SubmittedVariantAccessioningService submittedVariantAccessioningService,
             ClusteredVariantAccessioningService clusteredVariantAccessioningService,
             Long accessioningMonotonicInitSs,
             Long accessioningMonotonicInitRs) {
        return new SubmittedVariantDeprecationWriter(parameters.getAssemblyAccession(), mongoTemplate,
                                                     submittedVariantAccessioningService,
                                                     clusteredVariantAccessioningService,
                                                     accessioningMonotonicInitSs, accessioningMonotonicInitRs,
                                                     parameters.getDeprecationIdSuffix(),
                                                     parameters.getDeprecationReason());
    }
}
