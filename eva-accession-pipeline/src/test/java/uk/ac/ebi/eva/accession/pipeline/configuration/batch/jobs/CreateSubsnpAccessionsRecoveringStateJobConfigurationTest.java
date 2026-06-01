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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.entities.ContiguousIdBlock;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionReportWriter;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchJobRepositoryTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.MongoTestConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.BUILD_REPORT_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration.JOB_LAUNCHER_SUBSNP_ACCESSION_JOB;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class,
        MongoTestConfiguration.class, BatchJobRepositoryTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-recover-test.properties")
public class CreateSubsnpAccessionsRecoveringStateJobConfigurationTest extends MongoTestContainerHelper {

    public static final long UNCOMMITTED_ACCESSION = 5000000000L;

    private static final int EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF = 22;

    @Autowired
    private CountServiceParameters countServiceParameters;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    private MockRestServiceServer mockServer;

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private ContiguousIdBlockRepository blockRepository;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(JOB_LAUNCHER_SUBSNP_ACCESSION_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        this.cleanSlate();
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
    }

    @AfterEach
    public void tearDown() throws Exception {
        mongoTemplate.getDb().drop();
        this.cleanSlate();
    }

    public void cleanSlate() throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.VARIANTS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.CONTIGS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getFasta() + ".fai"));
    }

    @Test
    public void accessionJobShouldRecoverUncommittedAccessions() throws Exception {
        startWithAnAccessionInMongoNotCommittedInTheBlockService();

        runJob();

        assertEquals(1, repository.findByAccession(UNCOMMITTED_ACCESSION).size(),
                "The uncommitted accession should be assigned to only 1 object");

        assertCountsInMongo(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF + 1);
        assertCountsInVcfReport(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF);
        assertCountsInBlockService(EXPECTED_VARIANTS_ACCESSIONED_FROM_VCF + 1);
    }

    private void startWithAnAccessionInMongoNotCommittedInTheBlockService() {
        repository.deleteAll();
        SubmittedVariant model = new SubmittedVariant("assembly", 1111, "project", "contig", 100, "A", "T", null, false,
                false, false, false, null);
        SubmittedVariantEntity entity = new SubmittedVariantEntity(UNCOMMITTED_ACCESSION, "hash-10", model, 1);
        repository.save(entity);

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
        assertEquals(SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
        assertEquals(ACCESSIONING_SHUTDOWN_STEP, iterator.next().getStepName());
        assertEquals(BUILD_REPORT_STEP, iterator.next().getStepName());
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
        long committedAccessionsCount = block.getLastCommitted() - block.getFirstValue() + 1; // +1: inclusive interval
        assertEquals(expected, committedAccessionsCount);
    }
}
