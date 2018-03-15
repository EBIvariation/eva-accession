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
package uk.ac.ebi.ampt2d.accession.variant;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import uk.ac.ebi.ampt2d.accession.ApplicationConstants;
import uk.ac.ebi.ampt2d.accession.variant.persistence.VariantAccessioningDatabaseService;
import uk.ac.ebi.ampt2d.accession.variant.persistence.VariantAccessioningRepository;
import uk.ac.ebi.ampt2d.commons.autoconfigure.EnableSpringDataContiguousIdService;
import uk.ac.ebi.ampt2d.commons.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.generators.monotonic.persistence.service.ContiguousIdBlockService;

@Configuration
@ConditionalOnProperty(name = "services", havingValue = "variant-accession")
@EnableSpringDataContiguousIdService
@EntityScan({"uk.ac.ebi.ampt2d.accession.variant.persistence"})
@EnableJpaRepositories(
        basePackages = {"uk.ac.ebi.ampt2d.accession.variant.persistence"}
)
public class VariantAccessioningConfiguration {

    @Value("${" + ApplicationConstants.VARIANT_BLOCK_SIZE + "}")
    private long blockSize;

    @Value("${" + ApplicationConstants.VARIANT_ID + "}")
    private String variantId;

    @Value("${" + ApplicationConstants.APPLICATION_INSTANCE_ID + "}")
    private String applicationInstanceId;

    @Autowired
    private ContiguousIdBlockService contiguousIdBlockService;

    @Autowired
    private VariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockService service;

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
        return new MonotonicAccessionGenerator<>(blockSize, variantId, applicationInstanceId, service);
    }

}