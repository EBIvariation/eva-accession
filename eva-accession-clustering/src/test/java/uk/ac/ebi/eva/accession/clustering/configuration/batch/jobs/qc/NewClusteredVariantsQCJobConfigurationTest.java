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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc;

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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.ContiguousIdBlocksDataSourceConfiguration;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc.NewClusteredVariantsQCJobConfiguration.REPORT_MISSING_CVE_STEP;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_NEW_CLUSTERED_VARIANTS_QC;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        ContiguousIdBlocksDataSourceConfiguration.class})
@TestPropertySource("classpath:clustering-qc-test.properties")
public class NewClusteredVariantsQCJobConfigurationTest extends MongoTestContainerHelper {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/clusteredVariantEntityQC.json");
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Autowired
    @Qualifier(JOB_LAUNCHER_NEW_CLUSTERED_VARIANTS_QC)
    private JobLauncherTestUtils jobLauncherTestUtilsNewClusteredVariantsQC;

    @Test
    public void newClusteredVariantsQCJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtilsNewClusteredVariantsQC.launchJob();
        List<String> expectedSteps = Collections.singletonList(REPORT_MISSING_CVE_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    private void assertStepsExecuted(List<String> expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

}