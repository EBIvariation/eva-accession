/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.clustering.runner;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.DatabaseState;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasNaming;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.*;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class ClusteringCommandLineRunnerTest {

    private static final String TEST_DB = "test-db";

    private static final int TAXONOMY = 60711;

    private static String ASM1;

    private static String ASM2;

    private static final String PROJECT = "PRJ1";

    private static final String ACCESSION_ATTRIBUTE = "accession";

    private static final String MERGE_DESTINATION_ATTRIBUTE = "mergeInto";

    private static final String SPLIT_INTO_ATTRIBUTE = "splitInto";

    private static final String EVENT_TYPE_ATTRIBUTE = "eventType";

    private static final String REASON_ATTRIBUTE = "reason";

    private static final String INACTIVE_OBJECTS_PREFIX = "inactiveObjects";

    @Autowired
    private Long accessioningMonotonicInitSs;

    @Autowired
    private Long accessioningMonotonicInitRs;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSource datasource;

    @Autowired
    private ClusteringCommandLineRunner runner;

    @Autowired
    private VcfReader vcfReader;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private MonotonicAccessionGenerator<IClusteredVariant> clusteredVariantAccessionGenerator;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriter;

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongoOnlyFirstStep;

    // Current clustering sequence is:
    // generate merge split candidates from clustered variants -> perform merge
    // -> perform split -> cluster new variants
    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriterPreMergeAndSplit;

    @Autowired
    @Qualifier(NON_CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriterPostMergeAndSplit;

    private ItemReader<SubmittedVariantOperationEntity> rsMergeCandidatesReader;

    private ItemReader<SubmittedVariantOperationEntity> rsSplitCandidatesReader;

    @Autowired
    @Qualifier(RS_MERGE_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsMergeWriter;

    @Autowired
    @Qualifier(RS_SPLIT_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsSplitWriter;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());


    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static String originalVcfInputFilePath;

    private static String originalVcfContent;

    private static String originalRemappedFrom;

    private static File tempVcfInputFileToTestFailingJobs;

    private boolean originalInputParametersCaptured = false;

    private SubmittedVariantEntity evaSS1, dbsnpSS2, evaSS3, dbsnpSS4, evaSS5, dbsnpSS6, dbsnpSS7, evaSS8, evaSS8_old,
            evaSS9;

    private ClusteredVariantEntity dbsnpRS1, evaRS2, dbsnpRS3, evaRS4, dbsnpRS5;

    private MockRestServiceServer mockServer;

    private static class RSLocus {
        String assembly;
        String contig;
        long start;
        VariantType type;

        public RSLocus(String assembly, String contig, long start, VariantType type) {
            this.assembly = assembly;
            this.contig = contig;
            this.start = start;
            this.type = type;
        }

        public String getHash() {
            Function<IClusteredVariant, String> hashingFunction =
                    new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
            return hashingFunction.apply(
                    new ClusteredVariant(this.assembly, -1, this.contig, this.start, this.type, null,
                                         null));
        }
    }

    private RSLocus rsLocus1, rsLocus2, rsLocus3, rsLocus4, rsLocus4_old, rsLocus5;

    @Autowired
    private CountServiceParameters countServiceParameters;

    @Autowired
    private RestTemplate restTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @BeforeClass
    public static void initializeTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".vcf.gz");
    }

    @AfterClass
    public static void deleteTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs.delete();
    }

    @Before
    public void setUp() throws Exception {
        if (!originalInputParametersCaptured) {
            originalVcfInputFilePath = inputParameters.getVcf();
            originalVcfContent = getOriginalVcfContent(originalVcfInputFilePath);
            originalRemappedFrom = inputParameters.getRemappedFrom();
            writeToTempVCFFile(originalVcfContent);
            originalInputParametersCaptured = true;
        }
        ASM1 = inputParameters.getRemappedFrom();
        ASM2 = inputParameters.getAssemblyAccession();
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(CLUSTERING_FROM_VCF_JOB);
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
        inputParameters.setRemappedFrom(originalRemappedFrom);
        useOriginalVcfFile();

        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
        mongoTemplate.getDb().drop();
    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
        mongoTemplate.getDb().drop();
    }

    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json"})
    @DirtiesContext
    public void runFullClusteringFromMongoJobWithNoErrors() throws JobExecutionException {
        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        JobInstance currentJobInstance =
                CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_MONGO_JOB,
                                                           jobExplorer, inputParameters.toJobParameters())
                                      .getJobInstance();
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                                                            CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                                                            PROCESS_RS_MERGE_CANDIDATES_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                                                            PROCESS_RS_SPLIT_CANDIDATES_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                                                            CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                                                            CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                                                            BACK_PROPAGATE_NEW_RS_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                BACK_PROPAGATE_SPLIT_OR_MERGED_RS_STEP));
    }

    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json"})
    @DirtiesContext
    // For a clustering job involving non-remapped variants, only one step i.e.,
    // CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP should be executed
    public void runPartialClusteringFromMongoJobForNonRemappedVariants() throws JobExecutionException {
        inputParameters.setRemappedFrom(null);
        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        JobInstance currentJobInstance =
                CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_MONGO_JOB,
                                jobExplorer, inputParameters.toJobParameters()).getJobInstance();
        assertEquals(0, jobRepository.getStepExecutionCount(currentJobInstance,
                CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP));
        assertEquals(0, jobRepository.getStepExecutionCount(currentJobInstance,
                PROCESS_RS_MERGE_CANDIDATES_STEP));
        assertEquals(0, jobRepository.getStepExecutionCount(currentJobInstance,
                PROCESS_RS_SPLIT_CANDIDATES_STEP));
        assertEquals(0, jobRepository.getStepExecutionCount(currentJobInstance,
                CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP));
        assertEquals(1, jobRepository.getStepExecutionCount(currentJobInstance,
                CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP));
        assertEquals(0, jobRepository.getStepExecutionCount(currentJobInstance,
                BACK_PROPAGATE_NEW_RS_STEP));
        assertEquals(0, jobRepository.getStepExecutionCount(currentJobInstance,
                BACK_PROPAGATE_SPLIT_OR_MERGED_RS_STEP));
    }

    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json"})
    @DirtiesContext
    public void runProcessRemappedRSJobWithNoErrors() throws JobExecutionException {
        runner.setJobNames(PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    /*
        @see <a href="https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=2023161403/>
     */
    public void runProcessRemappedRSJobMultipleTimes() throws JobExecutionException {
        rsLocus1 = new RSLocus(ASM2, "chr1", 100L, VariantType.SNV);
        rsLocus2 = new RSLocus(ASM2, "chr1", 101L, VariantType.DEL);
        rsLocus3 = new RSLocus(ASM2, "chr1", 102L, VariantType.INS);
        rsLocus4 = new RSLocus(ASM2, "chr1", 103L, VariantType.SNV);
        rsLocus5 = new RSLocus(ASM2, "chr1", 104L, VariantType.INS);

        List<List<SubmittedVariantEntity>> mergeOperations = new ArrayList<>();
        List<List<SubmittedVariantEntity>> splitOperations = new ArrayList<>();
        List<String> insertedRSHashes = new ArrayList<>();
        // First run
        SubmittedVariantEntity ss1 = createSS(1L, 1L, rsLocus1, "A", "T", true);
        SubmittedVariantEntity ss2 = createSS(2L, 2L, rsLocus1, "A", "G", true);
        SubmittedVariantEntity ss3 = createSS(3L, 3L, rsLocus2, "T", "", true);
        runner.setJobNames(PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        mergeOperations.add(Arrays.asList(ss1, ss2));
        insertedRSHashes.addAll(Arrays.asList(rsLocus1.getHash(), rsLocus2.getHash()));
        assertDatabaseState(mergeOperations, splitOperations, insertedRSHashes);

        // Second run
        SubmittedVariantEntity ss4 = createSS(4L, 3L, rsLocus3, "", "T", true);
        SubmittedVariantEntity ss5 = createSS(5L, 4L, rsLocus4, "A", "G", true);
        runner.setJobNames(PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        splitOperations.add(Arrays.asList(ss3, ss4));
        insertedRSHashes.addAll(Arrays.asList(rsLocus3.getHash(), rsLocus4.getHash()));
        assertDatabaseState(mergeOperations, splitOperations, insertedRSHashes);

        // Third run
        SubmittedVariantEntity ss6 = createSS(6L, 5L, rsLocus4, "G", "A", true);
        runner.setJobNames(PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        mergeOperations.add(Arrays.asList(ss5, ss6));
        assertDatabaseState(mergeOperations, splitOperations, insertedRSHashes);

        // Fourth run
        createSS(7L, 6L, rsLocus5, "", "A", true);
        runner.setJobNames(PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        insertedRSHashes.add(rsLocus5.getHash());
        assertDatabaseState(mergeOperations, splitOperations, insertedRSHashes);
    }

    private void assertDatabaseState(List<List<SubmittedVariantEntity>> expectedSSListInMergeOps,
                                     List<List<SubmittedVariantEntity>> expectedSSListInSplitOps,
                                     List<String> expectedInsertedRSHashes) {
        List<SubmittedVariantOperationEntity> allOperationsInDB =
                this.mongoTemplate.findAll(SubmittedVariantOperationEntity.class);
        List<SubmittedVariantOperationEntity> mergeOperationsInDB = allOperationsInDB.stream().filter(
                op -> op.getEventType().equals(EventType.RS_MERGE_CANDIDATES)).collect(Collectors.toList());
        List<SubmittedVariantOperationEntity> splitOperationsInDB = allOperationsInDB.stream().filter(
                op -> op.getEventType().equals(EventType.RS_SPLIT_CANDIDATES)).collect(Collectors.toList());
        assertEquals(expectedSSListInMergeOps.size() , mergeOperationsInDB.size());
        assertEquals(splitOperationsInDB.size() , splitOperationsInDB.size());
        if (expectedSSListInMergeOps.size() > 0) {
            assertTrue(expectedSSListInMergeOps.stream()
                                              .allMatch(participantSSList ->
                                                                areAllSSPresentInOperation(participantSSList,
                                                                                           mergeOperationsInDB)
                                              ));
        }
        if (expectedSSListInSplitOps.size() > 0) {
            assertTrue(expectedSSListInSplitOps.stream()
                                              .allMatch(participantSSList ->
                                                                areAllSSPresentInOperation(participantSSList,
                                                                                           splitOperationsInDB)
                                              ));
        }
        List<ClusteredVariantEntity> clusteredVariantEntities =
                this.mongoTemplate.findAll(ClusteredVariantEntity.class);
        clusteredVariantEntities.addAll(this.mongoTemplate.findAll(DbsnpClusteredVariantEntity.class));
        assertEquals(expectedInsertedRSHashes.size(), clusteredVariantEntities.size());
        assertTrue(expectedInsertedRSHashes.containsAll(clusteredVariantEntities.stream()
                                                                      .map(ClusteredVariantEntity::getHashedMessage)
                                                                      .collect(Collectors.toList())));
    }

    private boolean areAllSSPresentInOperation(List<SubmittedVariantEntity> expectedParticipantSS,
                                               List<SubmittedVariantOperationEntity> operationsInDB) {
        List<Long> expectedParticipantSSAccessions = expectedParticipantSS
                .stream().map(SubmittedVariantEntity::getAccession).collect(Collectors.toList());
        List<String> expectedParticipantSSHashes = expectedParticipantSS
                .stream().map(SubmittedVariantEntity::getHashedMessage).collect(Collectors.toList());
        SubmittedVariantOperationEntity matchingOperation =
                operationsInDB.stream().filter(op -> op.getInactiveObjects().stream()
                                                   .anyMatch(inactiveEntity ->
                                                                     expectedParticipantSSHashes.contains(
                                                                             inactiveEntity.toSubmittedVariantEntity()
                                                                                     .getHashedMessage())))
                          .findFirst().get();
        // Match both SS hashes and accessions because SS hash equality does not check for accessions
        boolean result = (expectedParticipantSSHashes.size() == matchingOperation.getInactiveObjects().size());
        result = result && expectedParticipantSSHashes.containsAll(
                matchingOperation.getInactiveObjects()
                                 .stream()
                                 .map(SubmittedVariantInactiveEntity::
                                              getHashedMessage)
                                 .collect(Collectors.toList()));
        result = result && expectedParticipantSSAccessions.containsAll(
                matchingOperation.getInactiveObjects()
                                 .stream()
                                 .map(SubmittedVariantInactiveEntity::
                                              getAccession)
                                 .collect(Collectors.toList()));
        return result;

    }

    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json"})
    @DirtiesContext
    public void runClusterUnclusteredVariantsJobWithNoErrors() throws JobExecutionException {
        runner.setJobNames(CLUSTER_UNCLUSTERED_VARIANTS_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    /*
      @see <a href="https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1510337153"/>
     */
    public void runBackPropagateRSJobWithNoErrors() throws JobExecutionException,
            AccessionCouldNotBeGeneratedException {
        setupRSAndSSInNewAndRemappedAssemblies();
        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        // Ensure that no back-propagation was performed when SS-RS association was preserved after remapping to ASM2
        assertSSBackPropRSAssociation(evaSS1.getAccession(), null, ASM1);
        assertSSBackPropRSAssociation(evaSS9.getAccession(), null, ASM1);

        // Ensure that back-propagation was performed when SS-RS association
        // was NOT preserved after remapping to ASM2 due to splits or merges
        assertSSBackPropRSAssociation(dbsnpSS2.getAccession(), dbsnpRS1.getAccession(), ASM1);
        Long newRS2InASM2 = this.submittedVariantAccessioningService
                .getAllActiveByAssemblyAndAccessionIn(ASM2, Collections.singletonList(dbsnpSS7.getAccession()))
                .stream().findFirst().get().getData().getClusteredVariantAccession();
        assertSSBackPropRSAssociation(dbsnpSS7.getAccession(), newRS2InASM2, ASM1);
        assertSSBackPropRSAssociation(evaSS8.getAccession(), newRS2InASM2, ASM1);
    }

    @Test
    @DirtiesContext
    /*
        @see <a href="https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1454412665"/>
     */
    public void runClusteringMongoJobWithOverlappingMergesAndSplits() throws JobExecutionException,
            AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        rsLocus1 = new RSLocus(ASM2, "chr1", 100L, VariantType.SNV);
        rsLocus2 = new RSLocus(ASM2, "chr1", 101L, VariantType.DEL);
        rsLocus3 = new RSLocus(ASM2, "chr1", 102L, VariantType.SNV);

        ClusteredVariantEntity rs1 = createRS(1L, rsLocus1, true);
        ClusteredVariantEntity rs2 = createRS(2L, rsLocus2, true);
        ClusteredVariantEntity rs3 = createRS(3L, rsLocus3, true);

        SubmittedVariantEntity ss1 = createSS(1L, rs1.getAccession(), rsLocus1, "A", "T", true);
        SubmittedVariantEntity ss2 = createSS(2L, rs3.getAccession(), rsLocus1, "A", "G", true);
        SubmittedVariantEntity ss2_another = createSS(2L, rs3.getAccession(), rsLocus2, "A", "", true);
        SubmittedVariantEntity ss3 = createSS(3L, rs2.getAccession(), rsLocus2, "C", "", true);

        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        List<Long> existingRS = Arrays.asList(rs1.getAccession(), rs2.getAccession(), rs3.getAccession());
        List<ClusteredVariantEntity> newRSIDs = this.mongoTemplate.findAll(ClusteredVariantEntity.class)
                                                                  .stream()
                                                                  .filter(e -> !existingRS.contains(e.getAccession()))
                                                                  .collect(Collectors.toList());
        assertEquals(1, newRSIDs.size());
        ClusteredVariantEntity rsNew = newRSIDs.get(0);
        assertSSRSAssociation(ss1, rs1, rsLocus1);
        assertSSRSAssociation(ss2, rs1, rsLocus1);
        assertSSRSAssociation(ss2_another, rsNew, rsLocus2);
        assertSSRSAssociation(ss3, rsNew, rsLocus2);
    }

    @Test
    @DirtiesContext
    public void runClusteringMongoJobOnRemappedVariantsWithNoErrors() throws JobExecutionException,
            AccessionCouldNotBeGeneratedException, AccessionDoesNotExistException,
            AccessionMergedException, AccessionDeprecatedException {
         /*
        Initial state and expected results here (marked in green): https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=972582890
        */
        setupRSAndSS();
        createEVASS8InASM1WithUnassignedRS();

        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        // Ensure that the total number of SS IDs and RS IDs are as postulated above
        // newRS1 and newRS2 post-merge in the remapped assembly,
        // no RS created for back-propagated newRS2 in the original assembly
        assertEquals(2, this.mongoTemplate.findAll(ClusteredVariantEntity.class).size());
        // dbsnpRS1, dbsnpRS3 and dbsnpRS5 post-merge
        assertEquals(3, this.mongoTemplate.findAll(DbsnpClusteredVariantEntity.class).size());
        // Ensure that the number of SS records don't change
        assertEquals(4, this.mongoTemplate.findAll(DbsnpSubmittedVariantEntity.class).size());
        // Ensure that there is one SS record with the backpropagated RS
        assertEquals(1, this.mongoTemplate.findAll(SubmittedVariantEntity.class).
                stream().filter(a -> Objects.nonNull(a.getBackPropagatedVariantAccession())).count());

        // Test RS-locus associations
        assertRSLocusAssociation(dbsnpRS1, rsLocus1);
        assertRSLocusAssociation(dbsnpRS3, rsLocus3);
        assertRSLocusAssociation(dbsnpRS5, rsLocus5);
        AccessionWrapper<IClusteredVariant, String, Long> newRS1Wrapper =
                clusteredVariantAccessioningService.getByAccession(
                        submittedVariantAccessioningService.getByAccession(evaSS3.getAccession()).getData()
                                                           .getClusteredVariantAccession());
        ClusteredVariantEntity newRS1 = new ClusteredVariantEntity(newRS1Wrapper.getAccession(),
                                                                   newRS1Wrapper.getHash(),
                                                                   newRS1Wrapper.getData());
        Long newRS2Accession = submittedVariantAccessioningService.getAllActiveByAssemblyAndAccessionIn(
                ASM2, Collections.singletonList(evaSS8.getAccession())).get(0).getData().getClusteredVariantAccession();
        AccessionWrapper<IClusteredVariant, String, Long> newRS2Wrapper =
                clusteredVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(ASM2, Collections.singletonList(newRS2Accession)).get(0);
        ClusteredVariantEntity newRS2 = new ClusteredVariantEntity(newRS2Wrapper.getAccession(),
                                                                   newRS2Wrapper.getHash(),
                                                                   newRS2Wrapper.getData());
        assertRSLocusAssociation(newRS1, rsLocus2);
        assertRSLocusAssociation(newRS2, rsLocus4);

        // Test RS-SS associations
        assertSSRSAssociation(evaSS1, dbsnpRS1, rsLocus1);
        assertSSRSAssociation(dbsnpSS2, dbsnpRS1, rsLocus1);
        assertSSRSAssociation(evaSS3, newRS1, rsLocus2);
        assertSSRSAssociation(dbsnpSS4, dbsnpRS3, rsLocus3);
        assertSSRSAssociation(evaSS5, dbsnpRS3, rsLocus3);
        assertSSRSAssociation(dbsnpSS6, newRS2, rsLocus4);
        assertSSRSAssociation(dbsnpSS7, newRS2, rsLocus4);
        assertSSRSAssociation(evaSS8, newRS2, rsLocus4);
        // Ensure that RS is back-propagated to the old SS from which evaSS8 was remapped
        assertEquals(newRS2Accession,
                submittedVariantAccessioningService.getAllActiveByAssemblyAndAccessionIn(ASM1,
                Collections.singletonList(evaSS8_old.getAccession())).get(0).getData()
                        .getBackPropagatedVariantAccession());
        assertSSRSAssociation(evaSS9, dbsnpRS5, rsLocus5);

        // Test operations
        // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1402276771
        assertPostMergeRSOperation(evaRS2, dbsnpRS1);
        assertPostMergeSSOperation(dbsnpSS2, evaRS2, dbsnpRS1);
        assertPostMergeSSOperation(evaSS3, evaRS2, dbsnpRS1);
        assertPostMergeRSOperation(evaRS4, dbsnpRS3);
        assertPostMergeSSOperation(evaSS5, evaRS4, dbsnpRS3);
        assertPostMergeSSOperation(dbsnpSS6, evaRS4, dbsnpRS3);
        assertPostMergeSSOperation(dbsnpSS7, evaRS4, dbsnpRS3);

        assertPostSplitRSOperation(dbsnpRS1, newRS1);
        assertPostSplitSSOperation(evaSS3, dbsnpRS1, newRS1);
        assertPostSplitRSOperation(dbsnpRS3, newRS2);
        assertPostSplitSSOperation(dbsnpSS6, dbsnpRS3, newRS2);
        assertPostSplitSSOperation(dbsnpSS7, dbsnpRS3, newRS2);

        // After merge and splits are carried out, ensure that the operation is written
        // for the evaSS8 clustered with newRS2
        assertPostMergeAndSplitClusteringOperation(evaSS8, newRS2);
    }

    private void assertPostMergeRSOperation(ClusteredVariantEntity mergee, ClusteredVariantEntity mergeDestination) {
        Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                collectionToReadFrom = clusteringWriter.getClusteredOperationCollection(mergee.getAccession());
        Query queryForExistingMergeOperations = query(where(ACCESSION_ATTRIBUTE).is(mergee.getAccession()))
                .addCriteria(where(MERGE_DESTINATION_ATTRIBUTE)
                                     .is(mergeDestination.getAccession()))
                .addCriteria(where(EVENT_TYPE_ATTRIBUTE)
                                     .is(EventType.MERGED.toString()))
                .addCriteria(where(INACTIVE_OBJECTS_PREFIX + ".asm")
                                     .is(mergee.getAssemblyAccession()));
        List<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                existingOperations = this.mongoTemplate.find(queryForExistingMergeOperations, collectionToReadFrom);
        assertEquals(1, existingOperations.size());
    }

    private void assertPostMergeSSOperation(SubmittedVariantEntity ssReceivingMergedRS,
                                            ClusteredVariantEntity mergeeRS, ClusteredVariantEntity destinationRS) {
        Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                collectionToReadFrom =
                (clusteringWriter.isEvaSubmittedVariant(ssReceivingMergedRS) ?
                        SubmittedVariantOperationEntity.class : DbsnpSubmittedVariantOperationEntity.class);
        Query queryForExistingMergeOperations = query(
                where(ACCESSION_ATTRIBUTE).is(ssReceivingMergedRS.getAccession()))
                .addCriteria(where(EVENT_TYPE_ATTRIBUTE)
                                     .is(EventType.UPDATED.toString()))
                .addCriteria(where(INACTIVE_OBJECTS_PREFIX + ".seq")
                                     .is(ssReceivingMergedRS.getReferenceSequenceAccession()))
                .addCriteria(where(REASON_ATTRIBUTE)
                                     .regex(String.format(".+rs%s .+ rs%s\\.", mergeeRS.getAccession(),
                                                          destinationRS.getAccession())));
        List<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                existingOperations = this.mongoTemplate.find(queryForExistingMergeOperations, collectionToReadFrom);
        assertEquals(1, existingOperations.size());
    }

    private void assertPostSplitSSOperation(SubmittedVariantEntity ssReceivingSplitRS,
                                            ClusteredVariantEntity originalRS, ClusteredVariantEntity splitRS) {
        Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                collectionToReadFrom =
                (clusteringWriter.isEvaSubmittedVariant(ssReceivingSplitRS) ?
                        SubmittedVariantOperationEntity.class : DbsnpSubmittedVariantOperationEntity.class);
        Query queryForExistingSplitOperations = query(
                where(ACCESSION_ATTRIBUTE).is(ssReceivingSplitRS.getAccession()))
                .addCriteria(where(EVENT_TYPE_ATTRIBUTE)
                                     .is(EventType.UPDATED.toString()))
                .addCriteria(where(INACTIVE_OBJECTS_PREFIX + ".seq")
                                     .is(ssReceivingSplitRS.getReferenceSequenceAccession()))
                .addCriteria(where(REASON_ATTRIBUTE)
                                     .regex(String.format(".+ split RS rs%s .+ split from rs%s .+",
                                                          splitRS.getAccession(), originalRS.getAccession())));
        List<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                existingOperations = this.mongoTemplate.find(queryForExistingSplitOperations, collectionToReadFrom);
        assertEquals(1, existingOperations.size());
    }

    private void assertPostMergeAndSplitClusteringOperation(SubmittedVariantEntity ssClusteredWithNewRS,
                                                            ClusteredVariantEntity newRS) {
        Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                collectionToReadFrom =
                (clusteringWriter.isEvaSubmittedVariant(ssClusteredWithNewRS) ?
                        SubmittedVariantOperationEntity.class : DbsnpSubmittedVariantOperationEntity.class);
        Query queryForExistingClusterOperations = query(
                where(ACCESSION_ATTRIBUTE).is(ssClusteredWithNewRS.getAccession()))
                .addCriteria(where(EVENT_TYPE_ATTRIBUTE)
                                     .is(EventType.UPDATED.toString()))
                .addCriteria(where(INACTIVE_OBJECTS_PREFIX + ".seq")
                                     .is(ssClusteredWithNewRS.getReferenceSequenceAccession()))
                .addCriteria(where(REASON_ATTRIBUTE)
                                     .regex(String.format("Clustering submitted variant %s with rs%s",
                                                          ssClusteredWithNewRS.getAccession(), newRS.getAccession())));
        List<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                existingOperations = this.mongoTemplate.find(queryForExistingClusterOperations, collectionToReadFrom);
        assertEquals(1, existingOperations.size());
    }

    private void assertPostSplitRSOperation(ClusteredVariantEntity original, ClusteredVariantEntity splitInto) {
        Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                collectionToReadFrom = clusteringWriter.getClusteredOperationCollection(original.getAccession());
        Query queryForExistingSplitOperations = query(where(ACCESSION_ATTRIBUTE).is(original.getAccession()))
                .addCriteria(where(SPLIT_INTO_ATTRIBUTE)
                                     .is(splitInto.getAccession()))
                .addCriteria(where(EVENT_TYPE_ATTRIBUTE)
                                     .is(EventType.RS_SPLIT.toString()))
                .addCriteria(where(INACTIVE_OBJECTS_PREFIX + ".asm")
                                     .is(original.getAssemblyAccession()));
        List<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                existingOperations = this.mongoTemplate.find(queryForExistingSplitOperations, collectionToReadFrom);

        assertEquals(1, existingOperations.size());
    }

    private void assertRSLocusAssociation(ClusteredVariantEntity expectedRS, RSLocus rsLocus)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        List<AccessionWrapper<IClusteredVariant, String, Long>> rsEntriesInDB =
                clusteredVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(expectedRS.getAssemblyAccession(),
                                                              Collections.singletonList(expectedRS.getAccession()));
        assertEquals(1, rsEntriesInDB.size());
        IClusteredVariant rsEntryInDB = rsEntriesInDB.get(0).getData();
        assertEquals(rsLocus.assembly,  rsEntryInDB.getAssemblyAccession());
        assertEquals(rsLocus.contig,  rsEntryInDB.getContig());
        assertEquals(rsLocus.start,  rsEntryInDB.getStart());
        assertEquals(rsLocus.type,  rsEntryInDB.getType());
    }

    private void assertSSBackPropRSAssociation(Long ssID, Long expectedBackPropRS, String originalAssembly) {
        List<ISubmittedVariant> ssInOriginalAssembly =
                this.submittedVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(originalAssembly, Collections.singletonList(ssID))
                        .stream().map(AccessionWrapper::getData).collect(Collectors.toList());
        assertEquals(1, ssInOriginalAssembly.size());
        Long actualBackPropRS = ssInOriginalAssembly.get(0).getBackPropagatedVariantAccession();
        if (Objects.isNull(expectedBackPropRS)) {
            assertNull(actualBackPropRS);
        } else {
            assertEquals(expectedBackPropRS, actualBackPropRS);
        }
    }

    private void assertSSRSAssociation(SubmittedVariantEntity ss, ClusteredVariantEntity rs, RSLocus rsLocus)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        AccessionWrapper<ISubmittedVariant, String, Long> ssInDBWrapper =
                this.submittedVariantAccessioningService
                        .getAllByAccession(ss.getAccession(), ContigAliasNaming.INSDC)
                        .stream()
                        .filter(entity -> entity.getHash().equals(ss.getHashedMessage())).findFirst().get();
        SubmittedVariantEntity ssInDB = new SubmittedVariantEntity(ssInDBWrapper.getAccession(),
                                                                   ssInDBWrapper.getHash(),
                                                                   ssInDBWrapper.getData(),
                                                                   ssInDBWrapper.getVersion());
        assertEquals(rs.getAccession(), ssInDB.getClusteredVariantAccession());
        assertEquals(rsLocus.assembly, ssInDB.getReferenceSequenceAccession());
        assertEquals(rsLocus.contig, ssInDB.getContig());
        assertEquals(rsLocus.start, ssInDB.getStart());
        assertEquals(rsLocus.type, VariantClassifier.getVariantClassification(ssInDB.getReferenceAllele(),
                                                                              ssInDB.getAlternateAllele()));
    }

    private void setupRSAndSS() throws AccessionCouldNotBeGeneratedException {
        rsLocus1 = new RSLocus(ASM2, "chr1", 100L, VariantType.SNV);
        rsLocus2 = new RSLocus(ASM2, "chr1", 101L, VariantType.SNV);
        rsLocus3 = new RSLocus(ASM2, "chr1", 102L, VariantType.SNV);
        rsLocus4 = new RSLocus(ASM2, "chr1", 103L, VariantType.SNV);
        rsLocus4_old = new RSLocus(ASM1, "chr1", 113L + 1 + ThreadLocalRandom.current().nextLong(10),
                                   VariantType.SNV);
        rsLocus5 = new RSLocus(ASM2, "chr1", 104L, VariantType.SNV);
        dbsnpRS1 = createRS(1L, rsLocus1, true);
        evaRS2 = createRS(5L, rsLocus2, false);
        dbsnpRS3 = createRS(2L, rsLocus3, false);
        evaRS4 = createRS(6L, rsLocus3, true);
        dbsnpRS5 = createRS(3L, rsLocus5, true);

        evaSS1 = createSS(5L, dbsnpRS1.getAccession(), rsLocus1, "A", "T", true);
        dbsnpSS2 = createSS(1L, evaRS2.getAccession(), rsLocus1, "A", "G", true);
        evaSS3 = createSS(6L, evaRS2.getAccession(), rsLocus2, "C", "G", false);
        dbsnpSS4 = createSS(2L, dbsnpRS3.getAccession(), rsLocus3, "G", "A", false);
        evaSS5 = createSS(7L, evaRS4.getAccession(), rsLocus3, "G", "A", true);
        dbsnpSS6 = createSS(3L, evaRS4.getAccession(), rsLocus4, "T", "C", false);
        dbsnpSS7 = createSS(4L, evaRS4.getAccession(), rsLocus4, "T", "A", true);
        evaSS8 = createSS(8L, null, rsLocus4, "T", "G", true);
        evaSS9 = createSS(9L, dbsnpRS5.getAccession(), rsLocus5, "A", "T", true);

        // Reserve at least 10 accessions (by generating them) for existing RS IDs in the setup above
        // so that new RS IDs created are distinctly identifiable
        // This is the easiest way to exhaust the accessions because
        // the monotonic accession generator will only create accessions in the EVA collection (ClusteredVariantEntity)
        // and not the dbSNP collection (DbsnpClusteredVariantEntity)
        clusteredVariantAccessionGenerator.generateAccessions(10);
    }

    private void createEVASS8InASM1WithUnassignedRS() {
        SubmittedVariant evaSS8_old_sv_obj = new SubmittedVariant(evaSS8.getRemappedFrom(),
                                                                  evaSS8.getTaxonomyAccession(),
                                                                  evaSS8.getProjectAccession(),
                                                                  evaSS8.getContig(),
                                                                  rsLocus4_old.start,
                                                                  evaSS8.getReferenceAllele(),
                                                                  evaSS8.getAlternateAllele(), null);
        Function<ISubmittedVariant, String> hashingFunction =
                new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        evaSS8_old = new SubmittedVariantEntity(evaSS8.getAccession(), hashingFunction.apply(evaSS8_old_sv_obj),
                                                evaSS8_old_sv_obj, 1);
        this.mongoTemplate.insert(evaSS8_old, this.mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
    }

    private void setupRSAndSSInNewAndRemappedAssemblies() throws AccessionCouldNotBeGeneratedException {
        setupRSAndSS();
        List<SubmittedVariantEntity> variantsInRemappedAssembly =
                Arrays.asList(evaSS1, dbsnpSS2, evaSS5, dbsnpSS7, evaSS8, evaSS9);
        for (SubmittedVariantEntity variantInRemappedAssembly: variantsInRemappedAssembly) {
            // getStart is multiplied by 1000 so as to avoid accidental hash collision across the list of SS above
            // due to the same start positions
            RSLocus rsLocusObj = new RSLocus(ASM1, variantInRemappedAssembly.getContig(),
                    variantInRemappedAssembly.getStart()*1000
                            + ThreadLocalRandom.current().nextLong(10),
                    VariantType.SNV);
            ClusteredVariantEntity rsObj = createRS(variantInRemappedAssembly.getClusteredVariantAccession(),
                                                    rsLocusObj, false);
            createSS(variantInRemappedAssembly.getAccession(),
                     variantInRemappedAssembly.getClusteredVariantAccession(), rsLocusObj,
                     variantInRemappedAssembly.getReferenceAllele(), variantInRemappedAssembly.getAlternateAllele(),
                     false, ASM1);
        }
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, RSLocus rsLocus, String reference,
                                            String alternate, boolean remappedFromAnotherAssembly) {
        return createSS(ssAccession, rsAccession, rsLocus, reference, alternate, remappedFromAnotherAssembly, null);
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, RSLocus rsLocus, String reference,
                                            String alternate, boolean remappedFromAnotherAssembly,
                                            String sourceAssembly) {
        Function<ISubmittedVariant, String> hashingFunction =  new SubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        String assemblyToUse = (sourceAssembly == null) ? ASM2 : sourceAssembly;
        SubmittedVariant submittedVariant = new SubmittedVariant(assemblyToUse, TAXONOMY, PROJECT, rsLocus.contig,
                                                                 rsLocus.start, reference, alternate, rsAccession);
        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(ssAccession, hash, submittedVariant,
                                                                                   1);
        if (remappedFromAnotherAssembly) {
            submittedVariantEntity.setRemappedFrom(ASM1);
        }
        if (ssAccession >= accessioningMonotonicInitSs) {
            mongoTemplate.save(submittedVariantEntity, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        } else {
            mongoTemplate.save(submittedVariantEntity,
                               mongoTemplate.getCollectionName(DbsnpSubmittedVariantEntity.class));
        }
        return submittedVariantEntity;
    }

    private ClusteredVariantEntity createRS(Long rsAccession, RSLocus rsLocus, boolean remappedFromAnotherAssembly) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant clusteredVariant = new ClusteredVariant(rsLocus.assembly, TAXONOMY, rsLocus.contig,
                                                                 rsLocus.start, rsLocus.type, false, null);
        String hash = hashingFunction.apply(clusteredVariant);
        ClusteredVariantEntity clusteredVariantEntity = new ClusteredVariantEntity(rsAccession, hash, clusteredVariant);
        // Note that only SS ID entries are created post remapping.
        // If we should simulate that environment,
        // we should not create RS ID entries in the database for remapped variants
        // like we did for SS IDs. The RS IDs for the remapped assembly will be created after the clustering process.
        if (!remappedFromAnotherAssembly && Objects.nonNull(rsAccession)) {
            if (rsAccession >= accessioningMonotonicInitRs) {
                mongoTemplate.save(clusteredVariantEntity, mongoTemplate.getCollectionName(ClusteredVariantEntity.class));
            } else {
                mongoTemplate.save(clusteredVariantEntity,
                                   mongoTemplate.getCollectionName(DbsnpClusteredVariantEntity.class));
            }
        }
        return clusteredVariantEntity;
    }

    @Test
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    @DirtiesContext
    public void runJobWithNoErrors() throws JobExecutionException {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void runJobWithNoName() throws JobExecutionException {
        runner.setJobNames(null);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void runNonExistentJob() throws JobExecutionException {
        runner.setJobNames("NOT_EXISTENT_JOB");
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        inputParameters.setForceRestart(true);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void restartFailedJobThatIsAlreadyInTheRepository() throws Exception {
        useTempVcfFile();
        injectErrorIntoTempVcf();
        JobInstance failingJobInstance = runJobAandCheckResults();

        inputParameters.setForceRestart(true);
        remediateTempVcfError();
        runJobBAndCheckRestart(failingJobInstance);
    }

    private JobInstance runJobAandCheckResults() throws Exception {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_VCF_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                                                                         CLUSTERING_FROM_VCF_STEP);
        //Ensure that only the first batch was written (batch size is 2 and error was at line#4)
        assertEquals(inputParameters.getChunkSize(), stepExecution.getWriteCount());

        return currentJobInstance;
    }

    private void runJobBAndCheckRestart(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_VCF_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        assertNotEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());
    }

    @Test
    @DirtiesContext
    public void forceRestartButNoJobInTheRepository() throws Exception {
        inputParameters.setForceRestart(true);
        assertEquals(Collections.EMPTY_LIST, jobExplorer.getJobNames());
        runner.run();

        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void resumeFailingJobFromCorrectChunk() throws Exception {
        // Jobs A, B, C are run chronological order; A and C have SAME parameters;
        // A is the job that is run after VCF fault injection (as part of the runTestWithFaultInjection method),
        // therefore should fail.
        // B is a job run with the original VCF without any faults (run separately), therefore should succeed.
        // C is a job with the same parameters as A run after VCF fault remediation (as part of the
        // runTestWithFaultInjection method), therefore should resume A and succeed.

        useTempVcfFile();
        injectErrorIntoTempVcf();
        JobInstance failingJobInstance = runJobAandCheckResults();

        runJobBAndCheckResults();

        remediateTempVcfError();
        runJobCAndCheckResumption(failingJobInstance);
    }

    private void runJobBAndCheckResults() throws Exception {
        useOriginalVcfFile();
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        //Restore state so that Job C can continue running after fault remediation
        useTempVcfFile();
    }

    private void runJobCAndCheckResumption(JobInstance failingJobInstance) throws Exception {
        runner.run();
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_VCF_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                                                                         CLUSTERING_FROM_VCF_STEP);
        // Did we resume the previous failed job instance?
        assertEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());

        int numberOfLinesInVcf = getNumberOfLinesInVcfString(originalVcfContent);
        // Test resumption point - did we pick up where we left off?
        // Ensure all the batches other than the first batch were processed
        assertEquals(numberOfLinesInVcf - inputParameters.getChunkSize(), stepExecution.getWriteCount());
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    private void injectErrorIntoTempVcf() throws Exception {
        String modifiedVcfContent = originalVcfContent.replace("ss5000000004", "4ss--jibberish");
        // Inject error in the VCF file to cause processing to stop at variant#4
        writeToTempVCFFile(modifiedVcfContent);
    }

    private void remediateTempVcfError() throws Exception {
        writeToTempVCFFile(originalVcfContent);
    }

    private void useOriginalVcfFile() throws Exception {
        inputParameters.setVcf(originalVcfInputFilePath);
        vcfReader.setResource(FileUtils.getResource(new File(originalVcfInputFilePath)));
    }

    private void useTempVcfFile() throws Exception {
        // The following does not actually change the wiring of the vcfReader since the wiring happens before the tests
        // This setVcf is only to facilitate identifying jobs in the job repo by parameter
        // (those that use original vs temp VCF)
        inputParameters.setVcf(tempVcfInputFileToTestFailingJobs.getAbsolutePath());
        /*
             * Change the auto-wired VCF for VCFReader at runtime
             * Rationale:
             *  1) Why not use two test configurations, one for a VCF that fails validation and another for a VCF
             *  that won't and test resumption?
             *     Beginning Spring Boot 2, job resumption can only happen when input parameters to the restarted job
             *     is the same as the failed job.
             *     Therefore, a test to check resumption cannot have two different config files with different
             *     parameters.vcf.
             *     This test therefore creates a dynamic VCF and injects errors at runtime to the VCF thus preserving
             *     the VCF parameter but changing the VCF content.
             *  2) Why not artificially inject a VcfReader exception?
             *     This will preclude us from verifying job resumption from a precise line in the VCF.
         */
        vcfReader.setResource(FileUtils.getResource(tempVcfInputFileToTestFailingJobs));
    }

    private void writeToTempVCFFile(String modifiedVCFContent) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(tempVcfInputFileToTestFailingJobs.getAbsolutePath());
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(modifiedVCFContent.getBytes(StandardCharsets.UTF_8));
        gzipOutputStream.close();
    }

    private String getOriginalVcfContent(String inputVcfPath) throws Exception {
        StringBuilder originalVCFContent = new StringBuilder();

        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inputVcfPath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream));

        String read;
        while ((read = reader.readLine()) != null) {
            originalVCFContent.append(read).append(System.lineSeparator());
        }
        return originalVCFContent.toString();
    }

    private int getNumberOfLinesInVcfString(String vcfString) {
        return (int) Arrays.stream(vcfString.split(System.lineSeparator()))
                           .filter(line -> !line.startsWith("#"))
                           .count();
    }

    @Test
    @DirtiesContext
    // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=972582890
    // for scenario used in this test
    // There is no specific reason for choosing this scenario for testing other than the fact
    // there is a convenience method setupRSAndSS above to set up this scenario.
    public void testIdempotentSplitAndMergeIdentificationAndHandling() throws Exception {
        setupRSAndSS();

        runSplitAndMergeIdentificationAndHandlingSteps();
        DatabaseState databaseStateAfterFirstRun = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        runSplitAndMergeIdentificationAndHandlingSteps();
        DatabaseState databaseStateAfterSecondRun = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        runSplitAndMergeIdentificationAndHandlingSteps();
        DatabaseState databaseStateAfterThirdRun = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);

        assertEquals(databaseStateAfterSecondRun, databaseStateAfterFirstRun);
        assertEquals(databaseStateAfterThirdRun, databaseStateAfterFirstRun);
    }

    private void runSplitAndMergeIdentificationAndHandlingSteps()
            throws Exception {
        // Due to a bug in launching individual steps from a flow - https://github.com/spring-projects/spring-batch/issues/1311
        // the following cannot be executed directly, therefore we launch the entire job
        // jobLauncherTestUtilsFromMongo.launchStep(CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        jobLauncherTestUtilsFromMongoOnlyFirstStep.launchJob();
        List<SubmittedVariantOperationEntity> mergeCandidates = new ArrayList<>();
        List<SubmittedVariantOperationEntity> splitCandidates = new ArrayList<>();
        SubmittedVariantOperationEntity tempSVO;
        rsMergeCandidatesReader = new RSMergeAndSplitCandidatesReaderConfiguration()
                .rsMergeCandidatesReader(this.mongoTemplate, this.inputParameters);
        while((tempSVO = rsMergeCandidatesReader.read()) != null) {
            mergeCandidates.add(tempSVO);
        }
        rsMergeWriter.write(mergeCandidates);
        rsSplitCandidatesReader = new RSMergeAndSplitCandidatesReaderConfiguration()
                .rsSplitCandidatesReader(this.mongoTemplate, this.inputParameters);
        while((tempSVO = rsSplitCandidatesReader.read()) != null) {
            splitCandidates.add(tempSVO);
        }
        rsSplitWriter.write(splitCandidates);
    }
}