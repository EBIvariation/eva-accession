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
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionSaveMode;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationProperties;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationPropertiesConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.ContigAliasConfiguration;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.generators.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantOperationService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

@Configuration
@EnableSpringDataContiguousIdService
@Import({ApplicationPropertiesConfiguration.class, MongoConfiguration.class, ContigAliasConfiguration.class})
/**
 * Configuration required to accession and query clustered variants.
 *
 * TODO Support EVA clustered variants, see {@link SubmittedVariantAccessioningConfiguration} for reference
 */
public class ClusteredVariantAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ClusteredVariantAccessioningConfiguration.class);

    @Autowired
    private ClusteredVariantAccessioningRepository repository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private ClusteredVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository dbsnpOperationRepository;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private ContiguousIdBlockService blockService;

    @Autowired
    private ContigAliasService contigAliasService;

    @Value("${accessioning.clustered.categoryId}")
    private String categoryId;

    @Value("${accession.save.mode:SAVE_ALL_THEN_RESOLVE}")
    private AccessionSaveMode accessionSaveMode;

    @Bean
    public Long accessioningMonotonicInitRs() {
        return blockService.getBlockParameters(categoryId).getBlockStartValue();
    }

    @Bean("nonhumanActiveService")
    public ClusteredVariantAccessioningService clusteredVariantAccessioningService() {
        return new ClusteredVariantAccessioningService(clusteredVariantMonotonicAccessioningService(),
                                                       dbsnpClusteredVariantMonotonicAccessioningService(),
                                                       accessioningMonotonicInitRs(), contigAliasService);
    }

    @Bean
    public ClusteredVariantMonotonicAccessioningService clusteredVariantMonotonicAccessioningService() {
        return new ClusteredVariantMonotonicAccessioningService(clusteredVariantAccessionGenerator(),
                                                                clusteredVariantAccessioningDatabaseService(),
                                                                new ClusteredVariantSummaryFunction(),
                                                                new SHA1HashingFunction(),
                                                                accessionSaveMode);
    }

    @Bean
    public DbsnpClusteredVariantMonotonicAccessioningService dbsnpClusteredVariantMonotonicAccessioningService() {
        return new DbsnpClusteredVariantMonotonicAccessioningService(dbsnpClusteredVariantAccessionGenerator(),
                                                                     dbsnpClusteredVariantAccessioningDatabaseService(),
                                                                     new ClusteredVariantSummaryFunction(),
                                                                     new SHA1HashingFunction(),
                                                                     accessionSaveMode);
    }

    @Bean
    public MonotonicAccessionGenerator<IClusteredVariant> clusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        logger.debug("Using application properties: " + properties.toString());
        return new MonotonicAccessionGenerator<>(
                properties.getClustered().getCategoryId(),
                blockService,
                clusteredVariantAccessioningDatabaseService());
    }

    @Bean
    public DbsnpMonotonicAccessionGenerator<IClusteredVariant> dbsnpClusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        return new DbsnpMonotonicAccessionGenerator<>(properties.getClustered().getCategoryId(), blockService);
    }

    @Bean
    public ClusteredVariantAccessioningDatabaseService clusteredVariantAccessioningDatabaseService() {
        return new ClusteredVariantAccessioningDatabaseService(repository, clusteredVariantInactiveService());
    }

    @Bean
    public DbsnpClusteredVariantAccessioningDatabaseService dbsnpClusteredVariantAccessioningDatabaseService() {
        return new DbsnpClusteredVariantAccessioningDatabaseService(dbsnpRepository,
                                                                    dbsnpClusteredVariantInactiveService());
    }

    @Bean
    public ClusteredVariantOperationService clusteredVariantHistoryService() {
        return new ClusteredVariantOperationService(dbsnpClusteredVariantInactiveService(),
                                                    clusteredVariantInactiveService(),
                                                    contigAliasService);
    }

    @Bean
    public ClusteredVariantOperationRepository clusteredVariantOperationRepository() {
        return operationRepository;
    }

    @Bean
    public DbsnpClusteredVariantOperationRepository dbsnpClusteredVariantOperationRepository() {
        return dbsnpOperationRepository;
    }

    @Bean
    public ClusteredVariantInactiveService clusteredVariantInactiveService() {
        return new ClusteredVariantInactiveService(operationRepository, ClusteredVariantInactiveEntity::new,
                                                   ClusteredVariantOperationEntity::new);
    }

    @Bean("nonhumanInactiveService")
    public DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService() {
        return new DbsnpClusteredVariantInactiveService(dbsnpOperationRepository,
                                                        DbsnpClusteredVariantInactiveEntity::new,
                                                        DbsnpClusteredVariantOperationEntity::new);
    }
}
