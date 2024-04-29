/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.configuration.human;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.autoconfigure.EnableSpringDataContiguousIdService;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.configuration.ApplicationProperties;
import uk.ac.ebi.eva.accession.core.configuration.ContigAliasConfiguration;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.generators.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.HumanDbsnpClusteredVariantAccessionRepository;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.HumanDbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantOperationAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

@Configuration
@EnableSpringDataContiguousIdService
@Import({HumanMongoConfiguration.class, ContigAliasConfiguration.class})
public class HumanClusteredVariantAccessioningConfiguration {

    @Autowired
    private HumanDbsnpClusteredVariantAccessionRepository humanDbsnpRepository;

    @Autowired
    private HumanDbsnpClusteredVariantOperationRepository humanDbsnpClusteredVariantOperationRepository;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private ContiguousIdBlockService service;

    @Autowired
    private ContigAliasService contigAliasService;

    @Bean("humanActiveService")
    public HumanDbsnpClusteredVariantMonotonicAccessioningService humanDbsnpClusteredActiveVariantAccessioningService() {
        return new HumanDbsnpClusteredVariantMonotonicAccessioningService(
                dbsnpClusteredVariantAccessionGenerator(),
                humanDbsnpClusteredVariantAccessioningDatabaseService(),
                new ClusteredVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    private DbsnpMonotonicAccessionGenerator<IClusteredVariant> dbsnpClusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        return new DbsnpMonotonicAccessionGenerator<>(properties.getClustered().getCategoryId(), service);
    }

    private HumanDbsnpClusteredVariantAccessioningDatabaseService humanDbsnpClusteredVariantAccessioningDatabaseService() {
        return new HumanDbsnpClusteredVariantAccessioningDatabaseService(humanDbsnpRepository,
                                                                         dbsnpClusteredVariantInactiveService());
    }

    @Bean("humanOperationsService")
    public HumanDbsnpClusteredVariantOperationAccessioningService humanDbsnpClusteredVariantOperationAccessioningService() {
        return new HumanDbsnpClusteredVariantOperationAccessioningService(humanDbsnpClusteredVariantOperationRepository);
    }

    @Bean("humanService")
    public HumanDbsnpClusteredVariantAccessioningService humanDbsnpClusteredVariantAccessioningService() {
        return new HumanDbsnpClusteredVariantAccessioningService(humanDbsnpClusteredActiveVariantAccessioningService(),
                                                                 humanDbsnpClusteredVariantOperationAccessioningService(),
                                                                 contigAliasService);
    }

    @Bean
    public HumanDbsnpClusteredVariantOperationRepository dbsnpClusteredVariantOperationRepository() {
        return humanDbsnpClusteredVariantOperationRepository;
    }

    @Bean("humanInactiveService")
    public DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService() {
        return new DbsnpClusteredVariantInactiveService(humanDbsnpClusteredVariantOperationRepository,
                                                        DbsnpClusteredVariantInactiveEntity::new,
                                                        DbsnpClusteredVariantOperationEntity::new);
    }
}
