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
package uk.ac.ebi.eva.accession.dbsnp.configuration.steps;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.test.configuration.TestConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.test.BatchTestConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.VALIDATE_CONTIGS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, TestConfiguration.class})
@TestPropertySource("classpath:validate-contigs.properties")
public class ValidateContigsStepConfigurationTest {

    private static final int EXPECTED_PROCESSED_CONTIGS_COUNT = 3;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    @Test
    @DirtiesContext
    public void executeStep() throws IOException {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(VALIDATE_CONTIGS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(1, jobExecution.getStepExecutions().size());
        assertEquals(EXPECTED_PROCESSED_CONTIGS_COUNT,
                     jobExecution.getStepExecutions().iterator().next().getWriteCount());
    }

}
