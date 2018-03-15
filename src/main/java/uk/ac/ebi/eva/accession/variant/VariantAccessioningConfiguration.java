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
package uk.ac.ebi.eva.accession.variant;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import uk.ac.ebi.eva.accession.ApplicationProperties;
import uk.ac.ebi.eva.accession.variant.persistence.VariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.variant.persistence.VariantAccessioningRepository;
import uk.ac.ebi.ampt2d.commons.accession.autoconfigure.EnableSpringDataContiguousIdService;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.persistence.service.ContiguousIdBlockService;

@Configuration
@EnableSpringDataContiguousIdService
@EntityScan({"uk.ac.ebi.eva.accession.variant.persistence"})
@EnableJpaRepositories(
        basePackages = {"uk.ac.ebi.eva.accession.variant.persistence"}
)
public class VariantAccessioningConfiguration {

    @Autowired
    private VariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockService service;

    @Bean
    @ConfigurationProperties(prefix = "accessioning")
    public ApplicationProperties applicationProperties() {
        return new ApplicationProperties();
    }

    @Bean
    public VariantAccessioningService variantAccessionService() {
        return new VariantAccessioningService(variantAccessionGenerator(), variantAccessioningDatabaseService());
    }

    @Bean
    public VariantAccessioningDatabaseService variantAccessioningDatabaseService() {
        return new VariantAccessioningDatabaseService(repository);
    }

    @Bean
    public MonotonicAccessionGenerator<VariantModel> variantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties();
        return new MonotonicAccessionGenerator<>(
                properties.getVariant().getBlockSize(),
                properties.getVariant().getCategoryId(),
                properties.getInstanceId(),
                service);
    }

}