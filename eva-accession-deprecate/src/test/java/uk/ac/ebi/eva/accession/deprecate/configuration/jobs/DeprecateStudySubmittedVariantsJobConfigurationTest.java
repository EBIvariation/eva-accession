/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.configuration.jobs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.configuration.ContiguousIdBlocksDataSourceConfiguration;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchTestConfiguration;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        ContiguousIdBlocksDataSourceConfiguration.class})
@TestPropertySource("classpath:study-submitted-variants-test.properties")
public class DeprecateStudySubmittedVariantsJobConfigurationTest extends MongoTestContainerHelper {
    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        MongoTestDatabaseSetup.populateTestDB(this.mongoTemplate);
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    public void basicJobCompletion() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchJob();

        List<String> expectedSteps = Collections.singletonList(DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        MongoTestDatabaseSetup.assertPostDeprecationDatabaseState(this.mongoTemplate);
    }

    private void assertStepsExecuted(List expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }
}
