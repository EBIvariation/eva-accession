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
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionReportWriter;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.BUILD_REPORT_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
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

    @Test
    public void accessionJobShouldRecoverUncommittedAccessions() throws Exception {
        // Fill Uncommitted Accessions - accessions that are present in MongoDB but wasn't committed in the block service
        initializeMongoDbWithUncommittedAccessions();

        verifyInitialDBState();

        runAccessioningJob();

        verifyEndDBState();

        assertCountsInVcfReport(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF);
        assertCountsInMongo(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF + 85);
    }

    private void verifyInitialDBState() {
        // Contiguous Id Block DB:
        // Initial state of DB is 4 blocks are present but not "committed" in postgresql
        // Initialized using "resources/test-data/contiguous_id_blocks_recover_state_data.sql"
        // block id     first value     last value      last committed
        //  1           5000000000      5000000029      4999999999
        //  2           5000000030      5000000059      5000000029
        //  3           5000000060      5000000089      5000000059
        //  4           5000000090      5000000119      5000000089

        // Mongo DB
        // 85 accessions have been used in mongoDB but are not reflected in the block allocation table
        // 30 accessions belong to 1st block (5000000000 to 5000000029),
        // 25 to the 2nd block (5000000030 to 500000034 and 5000000040 to 5000000059)
        // 30 to the 3rd block (5000000060 to 5000000089)
        // None in 4th block
        assertEquals(85, mongoRepository.count());   // 30 + 25 + 30
        assertEquals(4, blockRepository.count());

        // Since none of the 4 blocks got committed - everyone's last committed value is first_value - 1
        ContiguousIdBlock block1 = blockRepository.findById(1l).get();
        assertEquals(5000000000l, block1.getFirstValue());
        assertEquals(4999999999l, block1.getLastCommitted());
        assertEquals(5000000029l, block1.getLastValue());
        assertEquals("test-instance-recover-state-00", block1.getApplicationInstanceId());
        assertTrue(block1.isNotReserved());

        ContiguousIdBlock block2 = blockRepository.findById(2l).get();
        assertEquals(5000000030l, block2.getFirstValue());
        assertEquals(5000000029l, block2.getLastCommitted());
        assertEquals(5000000059l, block2.getLastValue());
        assertEquals("test-instance-recover-state-00", block1.getApplicationInstanceId());
        assertTrue(block2.isNotReserved());

        ContiguousIdBlock block3 = blockRepository.findById(3l).get();
        assertEquals(5000000060l, block3.getFirstValue());
        assertEquals(5000000059l, block3.getLastCommitted());
        assertEquals(5000000089l, block3.getLastValue());
        assertEquals("test-instance-recover-state-00", block1.getApplicationInstanceId());
        assertTrue(block3.isNotReserved());

        ContiguousIdBlock block4 = blockRepository.findById(4l).get();
        assertEquals(5000000090l, block4.getFirstValue());
        assertEquals(5000000089l, block4.getLastCommitted());
        assertEquals(5000000119l, block4.getLastValue());
        assertEquals("test-instance-recover-state-00", block1.getApplicationInstanceId());
        assertTrue(block4.isNotReserved());
    }

    private void verifyEndDBState() {
        // VCF has 22 variants that needs to be accessioned

        assertEquals(107, mongoRepository.count());  // 85 (already present) + 22  (accessioned)
        assertEquals(4, blockRepository.count());

        /*
        * Accessions that were already present in mongo but not updated in block's last committed were recovered.
        * */

        // Block Recovered - (No accession used from this block as entire block was already used)
        ContiguousIdBlock block1 = blockRepository.findById(1l).get();
        assertEquals(5000000000l, block1.getFirstValue());
        assertEquals(5000000029l, block1.getLastCommitted());
        assertEquals(5000000029l, block1.getLastValue());
        assertEquals("test-instance-recover-state-01", block1.getApplicationInstanceId());
        assertTrue(block1.isNotReserved());

        // Block Recovered - (used the 5 unused accessions 5000000035 to 5000000039 and recovered others)
        ContiguousIdBlock block2 = blockRepository.findById(2l).get();
        assertEquals(5000000030l, block2.getFirstValue());
        assertEquals(5000000059l, block2.getLastCommitted());
        assertEquals(5000000059l, block2.getLastValue());
        assertEquals("test-instance-recover-state-01", block1.getApplicationInstanceId());
        assertTrue(block2.isNotReserved());

        // Block Recovered - (No accession used from this block as entire block was already used)
        ContiguousIdBlock block3 = blockRepository.findById(3l).get();
        assertEquals(5000000060l, block3.getFirstValue());
        assertEquals(5000000089l, block3.getLastCommitted());
        assertEquals(5000000089l, block3.getLastValue());
        assertEquals("test-instance-recover-state-01", block1.getApplicationInstanceId());
        assertTrue(block3.isNotReserved());

        // used the remaining 17 (22 - 5 (2nd block)) from 4th block
        ContiguousIdBlock block4 = blockRepository.findById(4l).get();
        assertEquals(5000000090l, block4.getFirstValue());
        assertEquals(5000000106l, block4.getLastCommitted());
        assertEquals(5000000119l, block4.getLastValue());
        assertEquals("test-instance-recover-state-01", block1.getApplicationInstanceId());
        assertTrue(block4.isNotReserved());
    }

    private void runAccessioningJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertStepNames(jobExecution.getStepExecutions());
    }

    private void assertStepNames(Collection<StepExecution> stepExecutions) {
        assertEquals(4, stepExecutions.size());
        Iterator<StepExecution> iterator = stepExecutions.iterator();
        assertEquals(CREATE_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
        assertEquals(ACCESSIONING_SHUTDOWN_STEP, iterator.next().getStepName());
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

    private void initializeMongoDbWithUncommittedAccessions() {
        mongoRepository.deleteAll();

        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        // Entries for 1st block
        for(long i=5000000000l;i<=5000000029l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }

        // Entries for 2nd block
        for(long i=5000000030l;i<=5000000034l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }
        for(long i=5000000040l;i<=5000000059l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }

        // Entries for 3rd block
        for(long i=5000000060l;i<=5000000089l;i++){
            SubmittedVariant model = new SubmittedVariant("assembly", 1111,
                    "project", "contig", 100, "A", "T",
                    null, false, false, false,
                    false, null);
            SubmittedVariantEntity entity = new SubmittedVariantEntity(i, "hash"+i, model, 1);
            submittedVariantEntityList.add(entity);
        }

        mongoRepository.saveAll(submittedVariantEntityList);
    }

}
