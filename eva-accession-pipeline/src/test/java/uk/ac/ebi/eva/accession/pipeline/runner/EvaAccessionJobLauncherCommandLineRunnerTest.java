/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.accession.pipeline.runner;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerTestConfiguration.TEST_JOB_NAME;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={RunnerTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class EvaAccessionJobLauncherCommandLineRunnerTest {

//    @Autowired
//    private JobLauncher jobLauncher;
//
//    @Autowired
//    private JobExplorer jobExplorer;

//    @Autowired
//    private InputParameters inputParameters;

    @Autowired
    EvaAccessionJobLauncherCommandLineRunner runner;

    @Test
    public void runJobWithNoName() throws Exception {
        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    public void runJobWithName() throws Exception {
        runner.setJobNames(TEST_JOB_NAME);
        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }
}