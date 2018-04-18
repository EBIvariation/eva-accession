/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.core.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.autoconfigure.EnableSpringDataContiguousIdService;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.persistence.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;

@Configuration
@EnableSpringDataContiguousIdService
@Import({MongoConfiguration.class})
public class SubmittedVariantAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SubmittedVariantAccessioningConfiguration.class);

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockService service;

    @Bean
    @ConfigurationProperties(prefix = "accessioning")
    public ApplicationProperties applicationProperties() {
        return new ApplicationProperties();
    }

    @Bean
    public SubmittedVariantAccessioningService submittedVariantAccessioningService() {
        return new SubmittedVariantAccessioningService(submittedVariantAccessionGenerator(),
                                                       submittedVariantAccessioningDatabaseService());
    }

    @Bean
    public SubmittedVariantAccessioningDatabaseService submittedVariantAccessioningDatabaseService() {
        return new SubmittedVariantAccessioningDatabaseService(repository);
    }

    @Bean
    public MonotonicAccessionGenerator<ISubmittedVariant> submittedVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties();
        logger.debug("Using application properties: " + properties.toString());
        return new MonotonicAccessionGenerator<>(
                properties.getVariant().getBlockSize(),
                properties.getVariant().getCategoryId(),
                properties.getInstanceId(),
                service);
    }

}
