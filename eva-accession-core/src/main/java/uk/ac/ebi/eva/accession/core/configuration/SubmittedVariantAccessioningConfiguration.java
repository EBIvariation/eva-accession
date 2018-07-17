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
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.DbsnpSubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.SubmittedVariantInactiveService;

@Configuration
@EnableSpringDataContiguousIdService
@Import({MongoConfiguration.class})
public class SubmittedVariantAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SubmittedVariantAccessioningConfiguration.class);

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private SubmittedVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpSubmittedVariantOperationRepository dbsnpOperationRepository;

    @Autowired
    private SubmittedVariantInactiveService inactiveService;

    @Autowired
    private DbsnpSubmittedVariantInactiveService dbsnpInactiveService;

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
                                                       submittedVariantAccessioningDatabaseService(),
                                                       dbsnpSubmittedVariantAccessioningDatabaseService());
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

    @Bean
    public SubmittedVariantAccessioningDatabaseService submittedVariantAccessioningDatabaseService() {
        return new SubmittedVariantAccessioningDatabaseService(repository, inactiveService);
    }

    @Bean
    public DbsnpSubmittedVariantAccessioningDatabaseService dbsnpSubmittedVariantAccessioningDatabaseService() {
        return new DbsnpSubmittedVariantAccessioningDatabaseService(dbsnpRepository, dbsnpInactiveService);
    }

    @Bean
    public SubmittedVariantOperationRepository submittedVariantOperationRepository() {
        return operationRepository;
    }

    @Bean
    public DbsnpSubmittedVariantOperationRepository dbsnpSubmittedVariantOperationRepository() {
        return dbsnpOperationRepository;
    }

    @Bean
    public SubmittedVariantInactiveService submittedVariantInactiveService() {
        return new SubmittedVariantInactiveService(operationRepository,
                                                   SubmittedVariantInactiveEntity::new,
                                                   SubmittedVariantOperationEntity::new);
    }

    @Bean
    public DbsnpSubmittedVariantInactiveService dbsnpSubmittedVariantInactiveService() {
        return new DbsnpSubmittedVariantInactiveService(dbsnpOperationRepository,
                                                        DbsnpSubmittedVariantInactiveEntity::new,
                                                        DbsnpSubmittedVariantOperationEntity::new);
    }

}
