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
package uk.ac.ebi.eva.accession.dbsnp2.runner;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_JOB;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class DbsnpJsonImportVariantsJobLauncherCommandLineRunnerTest {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DbsnpJsonImportVariantsJobLauncherCommandLineRunner runner;

    @Before
    public void setUp() throws Exception {
        runner.setJobNames(IMPORT_DBSNP_JSON_VARIANTS_JOB);
    }

    @Test
    @DirtiesContext
    public void runSuccessfulJob() throws Exception {
        inputParameters.setForceImport(true);
        runner.run();
        assertEquals(DbsnpJsonImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }
}