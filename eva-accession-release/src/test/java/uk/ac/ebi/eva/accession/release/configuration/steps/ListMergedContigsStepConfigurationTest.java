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
package uk.ac.ebi.eva.accession.release.configuration.steps;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.release.io.ContigWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_MERGED_CONTIGS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@UsingDataSet(locations = {"/test-data/dbsnpClusteredVariantOperationEntity.json"})
@TestPropertySource("classpath:application.properties")
public class ListMergedContigsStepConfigurationTest {

    private static final String TEST_DB = "test-db";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Autowired
    MongoTemplate mongoTemplate;

    @Before
    public void setUp() throws Exception {
        new File(ContigWriter.getMergedContigsFilePath(inputParameters.getOutputFolder(),
                                                       inputParameters.getAssemblyAccession()))
                .delete();
    }

    @After
    public void tearDown() throws Exception {
        new File(ContigWriter.getMergedContigsFilePath(inputParameters.getOutputFolder(),
                                                       inputParameters.getAssemblyAccession()))
                .delete();
    }

    @Test
    @DirtiesContext
    public void assertStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(LIST_MERGED_CONTIGS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    @DirtiesContext
    public void contigsWritten() throws Exception {
        assertStepExecutesAndCompletes();

        assertEquals(new HashSet<>(Arrays.asList("CM001954.1", "CM001941.2", "CM000346.1")),
                     setOfLines(ContigWriter.getMergedContigsFilePath(inputParameters.getOutputFolder(),
                                                                      inputParameters.getAssemblyAccession())));
    }

    private Set<String> setOfLines(String path) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        return bufferedReader.lines().collect(Collectors.toSet());
    }
}
