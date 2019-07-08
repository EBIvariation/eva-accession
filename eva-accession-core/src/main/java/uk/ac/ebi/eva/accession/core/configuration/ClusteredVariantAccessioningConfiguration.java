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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.autoconfigure.EnableSpringDataContiguousIdService;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.service.DbsnpClusteredVariantInactiveService;

@Configuration
@EnableSpringDataContiguousIdService
@Import({ApplicationPropertiesConfiguration.class, MongoConfiguration.class})
/**
 * Configuration required to accession and query clustered variants.
 *
 * TODO Support EVA clustered variants, see {@link SubmittedVariantAccessioningConfiguration} for reference
 */
public class ClusteredVariantAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ClusteredVariantAccessioningConfiguration.class);

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository dbsnpOperationRepository;

    @Autowired
    private DbsnpClusteredVariantInactiveService dbsnpInactiveService;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private ContiguousIdBlockService service;

    @Value("${accessioning.clustered.categoryId}")
    private String categoryId;

    @Bean
    public Long accessioningMonotonicInitSs() {
        return service.getBlockParameters(categoryId).getBlockStartValue();
    }

    @Bean
    public ClusteredVariantAccessioningService clusteredVariantAccessioningService() {
        return new ClusteredVariantAccessioningService(dbsnpClusteredVariantAccessionGenerator(),
                                                       dbsnpClusteredVariantAccessioningDatabaseService());
    }

    @Bean
    public MonotonicAccessionGenerator<IClusteredVariant> clusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        logger.debug("Using application properties: " + properties.toString());
        return new MonotonicAccessionGenerator<>(
                properties.getClustered().getCategoryId(),
                properties.getInstanceId(),
                service,
                (long[]) null);
    }

    @Bean
    public DbsnpMonotonicAccessionGenerator<IClusteredVariant> dbsnpClusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        return new DbsnpMonotonicAccessionGenerator<>(properties.getClustered().getCategoryId(),
                                                      properties.getInstanceId(), service);
    }

    @Bean
    public DbsnpClusteredVariantAccessioningDatabaseService dbsnpClusteredVariantAccessioningDatabaseService() {
        return new DbsnpClusteredVariantAccessioningDatabaseService(dbsnpRepository, dbsnpInactiveService);
    }

    @Bean
    public DbsnpClusteredVariantOperationRepository dbsnpClusteredVariantOperationRepository() {
        return dbsnpOperationRepository;
    }

    @Bean
    public DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService() {
        return new DbsnpClusteredVariantInactiveService(dbsnpOperationRepository,
                                                        DbsnpClusteredVariantInactiveEntity::new,
                                                        DbsnpClusteredVariantOperationEntity::new);
    }

}
