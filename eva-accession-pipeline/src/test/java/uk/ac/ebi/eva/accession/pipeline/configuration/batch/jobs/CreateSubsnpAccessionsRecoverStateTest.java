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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.entities.ContiguousIdBlock;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionReportWriter;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.pipeline.test.RecoverTestAccessioningConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.BUILD_REPORT_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {RecoverTestAccessioningConfiguration.class, BatchTestConfiguration.class,
        SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-recover-state-test.properties")
public class CreateSubsnpAccessionsRecoverStateTest {
    private static final String TEST_DB = "test-db";

    @Autowired
    private SubmittedVariantAccessioningRepository mongoRepository;

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

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @Autowired
    private CountServiceParameters countServiceParameters;

    private static final int EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF = 22;

    private MockRestServiceServer mockServer;

    @Before
    public void setUp() throws Exception {
        this.cleanSlate();
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
    }

    @After
    public void tearDown() throws Exception {
        this.cleanSlate();
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
    }

    public void cleanSlate() throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.VARIANTS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.CONTIGS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getFasta() + ".fai"));
    }

    /**
     * Note that for this test to work, we prepare the Mongo database in {@link RecoverTestAccessioningConfiguration}.
     */
    @Test
    public void accessionJobShouldRecoverUncommittedAccessions() throws Exception {
        verifyInitialDBState();

        runAccessioningJob();

        verifyEndDBState();

        assertCountsInVcfReport(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF);
        assertCountsInMongo(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF + 85);
    }

    /**
     * We initialize DB for the test by inserting sves in {@link RecoverTestAccessioningConfiguration}.
     */
    private void verifyInitialDBState() {
        // Contiguous Id Block DB:
        // Initial state of DB is 4 blocks are "reserved" but not "committed" in postgresql
        // block id     first value     last value      last committed
        //  1           5000000000      5000000029      4999999999
        //  2           5000000030      5000000059      5000000029
        //  3           5000000060      5000000089      5000000059
        //  4           5000000090      5000000119      5000000089

        // Mongo DB
        // 85 accessions have been used in mongoDB but are not reflected in the block allocation table
        // 30 accessions belong to 1st block (5000000000 to 5000000029),
        // 25 to the 2nd block (5000000035 to 5000000059)
        // 30 to the 3rd block (5000000060 to 5000000089)
        // None in 4th block
        assertEquals(85, mongoRepository.count());   // 30 + 25 + 30
        assertEquals(4, blockRepository.count());

        //recover state run in constructor of the MonotonicAccessionGenerator,
        // so will run when objects are being created even before job starts and will try to recover the blocks
        // The below stage of DB is after the recover state has already ran

        // 1st block is recovered and last committed updated to 5000000029 as all are present in mongo
        ContiguousIdBlock block1 = blockRepository.findById(1l).get();
        assertEquals(5000000000l, block1.getFirstValue());
        assertEquals(5000000029l, block1.getLastCommitted());
        assertEquals(5000000029l, block1.getLastValue());

        // 2nd block's committed is not updated as some accessions (5000000030 to 5000000034) are still available to use
        ContiguousIdBlock block2 = blockRepository.findById(2l).get();
        assertEquals(5000000030l, block2.getFirstValue());
        assertEquals(5000000029l, block2.getLastCommitted());
        assertEquals(5000000059l, block2.getLastValue());

        // 3rd block is not updated even though it is full (all accessions of this block are present in mongo)
        // the current algorithm takes the uncompleted blocks in ascending order of last value
        // and stops updating blocks as soon as it finds a block which should not be updated
        // In our case 1st is picked and updated,
        // 2nd is picked but should not be updated as it still has few accessions at the beginning which can be allotted
        // the algorithm stops at this point and did not bother to check the 3rd block
        ContiguousIdBlock block3 = blockRepository.findById(3l).get();
        assertEquals(5000000060l, block3.getFirstValue());
        assertEquals(5000000059l, block3.getLastCommitted());
        assertEquals(5000000089l, block3.getLastValue());

        ContiguousIdBlock block4 = blockRepository.findById(4l).get();
        assertEquals(5000000090l, block4.getFirstValue());
        assertEquals(5000000089l, block4.getLastCommitted());
        assertEquals(5000000119l, block4.getLastValue());
    }

    private void verifyEndDBState() {
        // VCF has 22 variants that needs to be accessioned

        assertEquals(107, mongoRepository.count());  // 85 (already present) + 22  (accessioned)
        assertEquals(4, blockRepository.count());

        ContiguousIdBlock block1 = blockRepository.findById(1l).get();
        assertEquals(5000000000l, block1.getFirstValue());
        assertEquals(5000000029l, block1.getLastCommitted());
        assertEquals(5000000029l, block1.getLastValue());

        // used the 5 unused accessions 5000000030 to 5000000034
        ContiguousIdBlock block2 = blockRepository.findById(2l).get();
        assertEquals(5000000030l, block2.getFirstValue());
        assertEquals(5000000059l, block2.getLastCommitted());
        assertEquals(5000000059l, block2.getLastValue());

        // Now that the 2nd block is full and committed, 3rd blocks also get's picked up and its last committed updated
        ContiguousIdBlock block3 = blockRepository.findById(3l).get();
        assertEquals(5000000060l, block3.getFirstValue());
        assertEquals(5000000089l, block3.getLastCommitted());
        assertEquals(5000000089l, block3.getLastValue());

        // used the remaining 17 (22 - 5 (2nd block)) from 4th block
        ContiguousIdBlock block4 = blockRepository.findById(4l).get();
        assertEquals(5000000090l, block4.getFirstValue());
        assertEquals(5000000106l, block4.getLastCommitted());
        assertEquals(5000000119l, block4.getLastValue());
    }

    private void runAccessioningJob() throws Exception {
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
        long numVariantsInMongo = mongoRepository.count();
        assertEquals(expected, numVariantsInMongo);
    }

    private void assertCountsInVcfReport(int expected) throws IOException {
        long numVariantsInReport = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(expected, numVariantsInReport);
    }

}
