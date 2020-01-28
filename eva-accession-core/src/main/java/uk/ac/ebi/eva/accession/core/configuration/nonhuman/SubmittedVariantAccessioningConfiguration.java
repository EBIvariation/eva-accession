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
package uk.ac.ebi.eva.accession.core.configuration.nonhuman;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.autoconfigure.EnableSpringDataContiguousIdService;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.configuration.ApplicationProperties;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationPropertiesConfiguration;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.generators.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

@Configuration
@EnableSpringDataContiguousIdService
@Import({ApplicationPropertiesConfiguration.class, MongoConfiguration.class})
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
    private ContiguousIdBlockService blockService;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Value("${accessioning.submitted.categoryId}")
    private String categoryId;

    @Bean
    public Long accessioningMonotonicInitSs() {
        return blockService.getBlockParameters(categoryId).getBlockStartValue();
    }

    @Bean
    public SubmittedVariantAccessioningService submittedVariantAccessioningService() {
        return new SubmittedVariantAccessioningService(submittedVariantMonotonicAccessioningService(),
                                                       dbsnpSubmittedVariantMonotonicAccessioningService(),
                                                       accessioningMonotonicInitSs());
    }

    private SubmittedVariantMonotonicAccessioningService submittedVariantMonotonicAccessioningService() {
        return new SubmittedVariantMonotonicAccessioningService(submittedVariantAccessionGenerator(),
                                                                submittedVariantAccessioningDatabaseService(),
                                                                new SubmittedVariantSummaryFunction(),
                                                                new SHA1HashingFunction());
    }

    private DbsnpSubmittedVariantMonotonicAccessioningService dbsnpSubmittedVariantMonotonicAccessioningService() {
        return new DbsnpSubmittedVariantMonotonicAccessioningService(dbsnpSubmittedVariantAccessionGenerator(),
                                                                     dbsnpSubmittedVariantAccessioningDatabaseService(),
                                                                     new SubmittedVariantSummaryFunction(),
                                                                     new SHA1HashingFunction());
    }

    @Bean
    public MonotonicAccessionGenerator<ISubmittedVariant> submittedVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        logger.debug("Using application properties: " + properties.toString());
        return new MonotonicAccessionGenerator<>(
                properties.getSubmitted().getCategoryId(),
                properties.getInstanceId(),
                blockService,
                submittedVariantAccessioningDatabaseService());
    }

    @Bean
    public DbsnpMonotonicAccessionGenerator<ISubmittedVariant> dbsnpSubmittedVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        return new DbsnpMonotonicAccessionGenerator<>(properties.getSubmitted().getCategoryId(),
                                                      properties.getInstanceId(),
                                                      blockService);
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
