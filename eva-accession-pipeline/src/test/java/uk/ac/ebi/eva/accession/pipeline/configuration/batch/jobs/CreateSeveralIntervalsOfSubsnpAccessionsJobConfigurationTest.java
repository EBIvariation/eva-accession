/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionReportWriter;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.BUILD_REPORT_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-interval-test.properties")
public class CreateSeveralIntervalsOfSubsnpAccessionsJobConfigurationTest {

    private static final int EXPECTED_VARIANTS = 22;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private CountServiceParameters countServiceParameters;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    private MockRestServiceServer mockServer;

    @Before
    public void setUp() throws Exception {
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                  .andExpect(method(HttpMethod.POST))
                  .andRespond(withStatus(HttpStatus.OK));
    }

    @After
    public void tearDown() throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.VARIANTS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + AccessionReportWriter.CONTIGS_FILE_SUFFIX));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getFasta() + ".fai"));
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
    }

    @Test
    @DirtiesContext
    public void executeJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertStepNames(jobExecution.getStepExecutions());

        long numVariantsInDatabase = repository.count();
        assertEquals(EXPECTED_VARIANTS, numVariantsInDatabase);

        long numVariantsInReport = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(EXPECTED_VARIANTS, numVariantsInReport);

        List<Integer> expectedIdsWithSkippedIntervals = Arrays.asList(500, 501, 502, 503, 504, 505, 506, 507, 508, 509,
                                                                      520, 521, 522, 523, 524, 525, 526, 527, 528, 529,
                                                                      540, 541);
        assertEquals(new TreeSet<>(expectedIdsWithSkippedIntervals),
                     getAccessionsFromReport(inputParameters.getOutputVcf()));
    }

    private void assertStepNames(Collection<StepExecution> stepExecutions) {
        assertEquals(4, stepExecutions.size());
        Iterator<StepExecution> iterator = stepExecutions.iterator();
        assertEquals(CREATE_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
        assertEquals(ACCESSIONING_SHUTDOWN_STEP, iterator.next().getStepName());
        assertEquals(BUILD_REPORT_STEP, iterator.next().getStepName());
        assertEquals(CHECK_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
    }

    private TreeSet<Integer> getAccessionsFromReport(String outputVcf) throws IOException {
        BufferedReader file = new BufferedReader(new InputStreamReader(new FileInputStream(outputVcf)));
        TreeSet<Integer> ids = new TreeSet<>();

        String line;
        while((line = file.readLine()) != null) {
            if (line.charAt(0) != '#') {
                String id = line.split("\t", 4)[2];
                ids.add(Integer.parseInt(id.substring(2)));
            }
        }
        return ids;
    }
}
