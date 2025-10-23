/*
 * Copyright 2025 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionWriter;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.QC_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration.JOB_LAUNCHER_QC_SUBSNP_JOB;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:qc-accession-pipeline-test.properties")
public class QCSubsnpAccessionsStepConfigurationTest {
    @Autowired
    @Qualifier(JOB_LAUNCHER_QC_SUBSNP_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private AccessionWriter accessionWriter;

    @MockBean
    private JobExecution jobExecution;

    @Before
    public void setUp() throws Exception {
        Mockito.when(jobExecution.getJobId()).thenReturn(1L);
        accessionWriter.setJobExecution(jobExecution);
    }

    @Test
    @DirtiesContext
    public void executeStep() {
        jobExecution = jobLauncherTestUtils.launchStep(QC_SUBSNP_ACCESSION_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        jobExecution.getStepExecutions().stream().filter(stepExec->stepExec.getStepName().equals(QC_SUBSNP_ACCESSION_STEP))
                .forEach(stepExec -> assertEquals(ExitStatus.COMPLETED, stepExec.getExitStatus()));
    }
}
