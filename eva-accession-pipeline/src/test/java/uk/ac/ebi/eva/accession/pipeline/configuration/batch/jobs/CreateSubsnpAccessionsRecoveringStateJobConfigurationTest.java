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

package uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.entities.ContiguousIdBlock;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionReportWriter;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.pipeline.test.RecoveringAccessioningConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.BUILD_REPORT_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {RecoveringAccessioningConfiguration.class, BatchTestConfiguration.class,
        SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-recover-test.properties")
public class CreateSubsnpAccessionsRecoveringStateJobConfigurationTest {

    public static final long UNCOMMITTED_ACCESSION = 5000000000L;

    private static final int EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF = 22;

    private static final String TEST_DB = "test-db";

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockRepository blockRepository;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    //needed for @UsingDataSet
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Before
    public void setUp() throws Exception {
        this.cleanSlate();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanSlate();
    }

    public void cleanSlate() throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.VARIANTS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.CONTIGS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getFasta() + ".fai"));
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
    }

    /**
     * Note that for this test to work, we prepare the Mongo database in {@link RecoveringAccessioningConfiguration}.
     */
    @Test
    public void accessionJobShouldRecoverUncommittedAccessions() throws Exception {
        startWithAnAccessionInMongoNotCommittedInTheBlockService();

        runJob();

        assertEquals("The uncommitted accession should be assigned to only 1 object",
                     1, repository.findByAccession(UNCOMMITTED_ACCESSION).size());

        assertCountsInMongo(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF + 1);
        assertCountsInVcfReport(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF);
        assertCountsInBlockService(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF + 1);
    }

    private void startWithAnAccessionInMongoNotCommittedInTheBlockService() {
        assertEquals(1, repository.count());
        assertEquals(1, repository.findByAccession(UNCOMMITTED_ACCESSION).size());
        assertEquals(1, blockRepository.count());

        // This means that the last committed accession is the previous one to the UNCOMMITTED_ACCESSION
        assertEquals(UNCOMMITTED_ACCESSION - 1, blockRepository.findAll().iterator().next().getLastCommitted());
    }

    private void runJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertStepNames(jobExecution.getStepExecutions());
    }

    private void assertStepNames(Collection<StepExecution> stepExecutions) {
        assertEquals(3, stepExecutions.size());
        Iterator<StepExecution> iterator = stepExecutions.iterator();
        assertEquals(CREATE_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
        assertEquals(BUILD_REPORT_STEP, iterator.next().getStepName());
        assertEquals(CHECK_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
    }

    private void assertCountsInMongo(int expected) {
        long numVariantsInMongo = repository.count();
        assertEquals(expected, numVariantsInMongo);
    }

    private void assertCountsInVcfReport(int expected) throws IOException {
        long numVariantsInReport = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(expected, numVariantsInReport);
    }

    private void assertCountsInBlockService(int expected) {
        assertEquals(1, blockRepository.count());
        ContiguousIdBlock block = blockRepository.findAll().iterator().next();
        long committedAccessionsCount = block.getLastCommitted() - block.getFirstValue() +1; // +1: inclusive interval
        assertEquals(expected, committedAccessionsCount);
    }
}
