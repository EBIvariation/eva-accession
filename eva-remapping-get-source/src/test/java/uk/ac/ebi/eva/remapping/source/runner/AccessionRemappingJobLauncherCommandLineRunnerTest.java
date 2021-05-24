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
package uk.ac.ebi.eva.remapping.source.runner;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;
import uk.ac.ebi.eva.remapping.source.parameters.ReportPathResolver;
import uk.ac.ebi.eva.remapping.source.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.remapping.source.test.configuration.MongoTestConfiguration;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/submittedVariantEntity.json"})
@TestPropertySource("classpath:application.properties")
public class AccessionRemappingJobLauncherCommandLineRunnerTest {

    private static final String TEST_DB = "test-db";

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private AccessionRemappingJobLauncherCommandLineRunner runner;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        deleteOutputFiles();
    }

    @After
    public void tearDown() {
        deleteOutputFiles();
    }

    private void deleteOutputFiles() {
        ReportPathResolver.getDbsnpReportPath(inputParameters.getOutputFolder(),
                                              inputParameters.getAssemblyAccession())
                          .toFile().delete();
        ReportPathResolver.getEvaReportPath(inputParameters.getOutputFolder(),
                                            inputParameters.getAssemblyAccession())
                          .toFile().delete();
    }

    @Test
    public void contextLoads() throws JobExecutionException {
        runner.run();
        assertEquals(AccessionRemappingJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }
}