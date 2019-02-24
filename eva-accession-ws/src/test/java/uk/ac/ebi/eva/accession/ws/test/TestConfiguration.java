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
package uk.ac.ebi.eva.accession.ws.test;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.DbsnpSubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

@Configuration
public class TestConfiguration {

    @Bean
    public DbsnpSubmittedVariantMonotonicAccessioningService dbsnpService(
            DbsnpMonotonicAccessionGenerator<ISubmittedVariant> dbsnpSubmittedVariantAccessionGenerator,
            DbsnpSubmittedVariantAccessioningDatabaseService dbsnpSubmittedVariantAccessioningDatabaseService) {
        return new DbsnpSubmittedVariantMonotonicAccessioningService(dbsnpSubmittedVariantAccessionGenerator,
                                                                     dbsnpSubmittedVariantAccessioningDatabaseService,
                                                                     new SubmittedVariantSummaryFunction(),
                                                                     new SHA1HashingFunction());
    }
}
