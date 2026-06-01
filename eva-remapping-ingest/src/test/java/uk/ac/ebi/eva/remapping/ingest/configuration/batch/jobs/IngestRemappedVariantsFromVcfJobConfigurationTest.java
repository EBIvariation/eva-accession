/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.configuration.batch.jobs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.remapping.ingest.batch.tasklets.RemappingMetadata;
import uk.ac.ebi.eva.remapping.ingest.test.configuration.BatchJobRepositoryTestConfiguration;
import uk.ac.ebi.eva.remapping.ingest.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.remapping.ingest.test.configuration.MongoTestConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP;
import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.STORE_REMAPPING_METADATA_STEP;
import static uk.ac.ebi.eva.remapping.ingest.test.configuration.BatchTestConfiguration.JOB_INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        BatchJobRepositoryTestConfiguration.class})
@TestPropertySource("classpath:ingest-remapped-variants.properties")
public class IngestRemappedVariantsFromVcfJobConfigurationTest extends MongoTestContainerHelper {

    @Autowired
    @Qualifier(JOB_INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/submittedVariantEntity.json");
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    public void jobFromVcf() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        List<String> expectedSteps = Arrays.asList(STORE_REMAPPING_METADATA_STEP,
                INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertMetadataAssociatedToSubmittedVariants();
    }

    private void assertStepsExecuted(List expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

    private void assertMetadataAssociatedToSubmittedVariants() {
        RemappingMetadata metadata = mongoTemplate.findOne(new Query(), RemappingMetadata.class);
        assertNotNull(metadata);
        String remappingId = metadata.getHashedMessage();

        Query remappedQuery = new Query(Criteria.where("remappedFrom").exists(true));
        List<SubmittedVariantEntity> variantsRemapped = mongoTemplate.find(remappedQuery, SubmittedVariantEntity.class);

        long variantsWithMetatada = variantsRemapped.stream()
                .filter(x -> x.getRemappingId() != null &&
                        x.getRemappingId().equals(remappingId))
                .count();
        assertEquals(4, variantsWithMetatada);
    }
}