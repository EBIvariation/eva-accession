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
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationProperties;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.generators.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.DbsnpClusteredHumanVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.DbsnpClusteredHumanVariantOperationAccessioningService;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.DbsnpClusteredHumanVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.DbsnpHumanClusteredVariantAccessionRepository;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.DbsnpClusteredHumanVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;

@Configuration
@EnableSpringDataContiguousIdService
@Import({MongoHumanConfiguration.class})
public class ClusteredHumanVariantAccessioningConfiguration {

    @Autowired
    private DbsnpHumanClusteredVariantAccessionRepository dbsnpHumanRepository;

    @Autowired
    private DbsnpClusteredHumanVariantOperationRepository dbsnpClusteredHumanVariantOperationRepository;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private ContiguousIdBlockService service;

    @Bean("humanActiveService")
    public ClusteredVariantAccessioningService dbsnpClusteredHumanActiveVariantAccessioningService() {
        return new ClusteredVariantAccessioningService(dbsnpClusteredVariantAccessionGenerator(),
                                                       dbsnpClusteredVariantAccessioningDatabaseService());
    }

    private DbsnpMonotonicAccessionGenerator<IClusteredVariant> dbsnpClusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        return new DbsnpMonotonicAccessionGenerator<>(properties.getClustered().getCategoryId(),
                                                      properties.getInstanceId(), service);
    }

    private DbsnpClusteredHumanVariantAccessioningDatabaseService dbsnpClusteredVariantAccessioningDatabaseService() {
        return new DbsnpClusteredHumanVariantAccessioningDatabaseService(dbsnpHumanRepository,
                dbsnpClusteredVariantInactiveService());
    }

    @Bean("humanOperationsService")
    public DbsnpClusteredHumanVariantOperationAccessioningService dbsnpClusteredHumanVariantOperationAccessioningService() {
        return new DbsnpClusteredHumanVariantOperationAccessioningService(dbsnpClusteredHumanVariantOperationRepository);
    }

    @Bean("humanService")
    public DbsnpClusteredHumanVariantAccessioningService dbsnpClusteredHumanVariantAccessioningService() {
        return new DbsnpClusteredHumanVariantAccessioningService(dbsnpClusteredHumanActiveVariantAccessioningService(),
                dbsnpClusteredHumanVariantOperationAccessioningService());
    }

    @Bean
    public DbsnpClusteredHumanVariantOperationRepository dbsnpClusteredVariantOperationRepository() {
        return dbsnpClusteredHumanVariantOperationRepository;
    }

    @Bean("humanInactiveService")
    public DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService() {
        return new DbsnpClusteredVariantInactiveService(dbsnpClusteredHumanVariantOperationRepository,
                DbsnpClusteredVariantInactiveEntity::new,
                DbsnpClusteredVariantOperationEntity::new);
    }
}
