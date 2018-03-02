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
package uk.ac.ebi.ampt2d.test.configurationaccession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import uk.ac.ebi.ampt2d.accession.ApplicationConstants;
import uk.ac.ebi.ampt2d.accession.variant.VariantAccessioningService;
import uk.ac.ebi.ampt2d.accession.variant.VariantModel;
import uk.ac.ebi.ampt2d.accession.variant.persistence.VariantAccessioningDatabaseService;
import uk.ac.ebi.ampt2d.accession.variant.persistence.VariantAccessioningRepository;
import uk.ac.ebi.ampt2d.accessioning.commons.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.accessioning.commons.generators.monotonic.persistence.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.ampt2d.accessioning.commons.generators.monotonic.persistence.service.ContiguousIdBlockService;

@TestConfiguration
public class VariantAccessioningDatabaseServiceTestConfiguration {

    @Autowired
    private VariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockRepository contiguousIdBlockRepository;

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
        return new MonotonicAccessionGenerator<>(1000L, "var-test", "test-inst",
                contiguousIdBlockService());
    }

    @Bean
    public ContiguousIdBlockService contiguousIdBlockService(){
        return new ContiguousIdBlockService(contiguousIdBlockRepository);
    }

}