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
package uk.ac.ebi.eva.accession.pipeline.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationProperties;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * This configuration class has the single purpose of having loaded in MongoDB an object *before* the
 * MonotonicAccessionGenerator is instantiated (and autowired in the accessioning service and pipeline jobs) so that
 * the generator can recover from uncommitted accessions.
 *
 * An uncommitted accession is an accession that is present in MongoDB but wasn't committed in the block service (e.g.
 * due to an unexpected crash of the application in previous executions). If the block service doesn't recover, this
 * might lead to a single accession being assigned to several different objects in mongo.
 */
@Configuration
public class RecoverTestAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(RecoveringAccessioningConfiguration.class);

    @Bean
    public SubmittedVariantMonotonicAccessioningService submittedVariantMonotonicAccessioningService(
            @Autowired @Qualifier("testSubmittedVariantAccessionGeneratorRecover")
            MonotonicAccessionGenerator<ISubmittedVariant> accessionGenerator,
            @Autowired SubmittedVariantAccessioningDatabaseService databaseService) {
        return new SubmittedVariantMonotonicAccessioningService(accessionGenerator,
                databaseService,
                new SubmittedVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    @Bean("testSubmittedVariantAccessionGeneratorRecover")
    public MonotonicAccessionGenerator<ISubmittedVariant> testSubmittedVariantAccessionGeneratorRecover(
            @Autowired SubmittedVariantAccessioningRepository repository,
            @Autowired SubmittedVariantAccessioningDatabaseService databaseService,
            @Autowired ApplicationProperties properties,
            @Autowired ContiguousIdBlockService blockService) {

        repository.deleteAll();

        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        // Entries for 1st block
        for(long i=5000000000l;i<=5000000029l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }

        // Entries for 2nd block
        for(long i=5000000035l;i<=5000000059l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }

        // Entries for 3rd block
        for(long i=5000000060l;i<=5000000089l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }

        repository.saveAll(submittedVariantEntityList);

        return new MonotonicAccessionGenerator<>(properties.getSubmitted().getCategoryId(),
                properties.getInstanceId(),
                blockService,
                databaseService);
    }
}
