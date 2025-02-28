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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import org.mockito.Mockito;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.clustering.test.DatabaseState;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:merge-split-test.properties")
@ContextConfiguration(classes = {RSMergeAndSplitCandidatesReaderConfiguration.class, RSMergeAndSplitWriterConfiguration.class,
        MongoTestConfiguration.class, BatchTestConfiguration.class})
public class RSSplitWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private File rsReportFile;

    @Autowired
    @Qualifier(RS_SPLIT_WRITER)
    private RSSplitWriter rsSplitWriter;

    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriter;

    @MockBean
    private JobExecution jobExecution;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private SubmittedVariantEntity ss1, ss2, ss3, ss4, ss5, ss6;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    private MetricCompute<ClusteringMetric> metricCompute;

    @Before
    public void setUp() throws IOException {
        cleanup();

        Mockito.when(jobExecution.getJobId()).thenReturn(1L);
        rsSplitWriter.setJobExecution(jobExecution);
        clusteringWriter.setJobExecution(jobExecution);
    }

    @After
    public void tearDown() throws IOException {
        cleanup();
    }

    private void cleanup() throws IOException {
        mongoClient.dropDatabase(TEST_DB);
        metricCompute.clearCount();
        Files.deleteIfExists(this.rsReportFile.toPath());
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate) {
        Function<ISubmittedVariant, String> hashingFunction = new SubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        SubmittedVariant temp = new SubmittedVariant(ASSEMBLY, 60711, "PRJ1", "chr1", start, reference, alternate,
                                                     rsAccession);
        return new SubmittedVariantEntity(ssAccession, hashingFunction.apply(temp), temp, 1);
    }

    @Test
    public void testSplitsWithUnequalNumberOfDistinctHashes() throws Exception {
        Long sameRSAccessionToBeUsedForDifferentLoci = 1L;
        // Create SS with the same RS with ss2 and ss3 sharing the same locus (chr + start + type)
        ss1 = createSS(1L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "T");
        ss2 = createSS(2L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "T");
        ss3 = createSS(3L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "G");
        ss4 = createSS(4L, sameRSAccessionToBeUsedForDifferentLoci, 102L, "A", "C");

        // Create multiple entries in clustered variant collection - one for each distinct loci but with the same accession
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = Stream.of(ss1, ss2, ss4)
                                                                       .map(clusteringWriter::toClusteredVariantEntity)
                                                                       .collect(Collectors.toList());

        mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DbsnpSubmittedVariantEntity.class);
        mongoTemplate.insert(Collections.singletonList(ss4), SubmittedVariantEntity.class);
        mongoTemplate.insert(multipleEntriesWithSameRS, DbsnpClusteredVariantEntity.class);
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                            ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                            Stream.of(ss1, ss2, ss3, ss4).map(SubmittedVariantInactiveEntity::new)
                                  .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));

        // Per splitting policy, ensure that SS2 and SS3 get to retain the old RS
        // because the RS hash for start position 101 is supported in these 2 variants
        List<AccessionWrapper<ISubmittedVariant, String, Long>> ssAssociatedWithRS1 =
        submittedVariantAccessioningService.getByClusteredVariantAccessionIn(
                Collections.singletonList(sameRSAccessionToBeUsedForDifferentLoci));
        assertEquals(2, ssAssociatedWithRS1.size());
        assertTrue(ssAssociatedWithRS1.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet())
                                      .containsAll(Arrays.asList(ss2.getAccession(), ss3.getAccession())));
        /* Check clustering counts */
        // 2 new RS IDs created for hashes with start positions 100 and 102
        assertEquals(2, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_CREATED));
        // 2 RS split events for hashes with start positions 100 and 102
        assertEquals(2, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_RS_SPLIT));
        // 2 new RS IDs associated with ss1 and ss4
        assertEquals(2, metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATE_OPERATIONS));
        assertEquals(2, metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATED_RS));

        List<String> rsReportLines = Files.readAllLines(this.rsReportFile.toPath());
        assertEquals(2, rsReportLines.size());
        // RS ID created for a given hash in not deterministic during the split.
        // So, use regex to check for just the hashes in the RS report.
        assertTrue(rsReportLines.stream()
                                .anyMatch(line -> line.matches(".*\t20610F413155992F45A41FB1DC6005B2FAA1565B")));
        assertTrue(rsReportLines.stream()
                                .anyMatch(line -> line.matches(".*\t717DE940ADEB9E9DCAF09600065BAAF9A91EE713")));
    }

    @Test
    public void testSplitsWithEqualNumberOfDistinctHashes() throws Exception {
        Long sameRSAccessionToBeUsedForDifferentLoci = 1L;
        // Each RS locus (chr + start + type) is supported by two SS
        ss1 = createSS(1L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "T");
        ss2 = createSS(2L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "A");
        ss3 = createSS(3L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "T");
        ss4 = createSS(4L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "G");
        ss5 = createSS(5L, sameRSAccessionToBeUsedForDifferentLoci, 102L, "G", "T");
        ss6 = createSS(6L, sameRSAccessionToBeUsedForDifferentLoci, 102L, "G", "C");

        // Create multiple entries in clustered variant collection - one for each distinct loci but with the same accession
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = Stream.of(ss1, ss3, ss5)
                                                                       .map(clusteringWriter::toClusteredVariantEntity)
                                                                       .collect(Collectors.toList());

        mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DbsnpSubmittedVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(ss4, ss5, ss6), SubmittedVariantEntity.class);
        mongoTemplate.insert(multipleEntriesWithSameRS, DbsnpClusteredVariantEntity.class);
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                            ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                            Stream.of(ss1, ss2, ss3, ss4, ss5, ss6).map(SubmittedVariantInactiveEntity::new)
                                  .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));

        // Per splitting policy, ensure that SS1 and SS2 get to retain the old RS
        // Even though locus with start 100, 101 and 102 have equal number of supporting variants
        // the group SS1 and SS2 with locus 100 has the oldest SS ID and gets to keep the old RS ID
        List<AccessionWrapper<ISubmittedVariant, String, Long>> ssAssociatedWithRS1 =
                submittedVariantAccessioningService.getByClusteredVariantAccessionIn(
                        Collections.singletonList(sameRSAccessionToBeUsedForDifferentLoci));
        assertEquals(2, ssAssociatedWithRS1.size());
        assertTrue(ssAssociatedWithRS1.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet())
                                      .containsAll(Arrays.asList(ss1.getAccession(), ss2.getAccession())));
        /* Check clustering counts */
        // 2 entries created for hashes with start positions 100 and 102
        assertEquals(2, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_CREATED));
        // 2 RS split events for hashes with start positions 100 and 102
        assertEquals(2, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_RS_SPLIT));
        // 2 new RS IDs associated with SS groups: ss3,ss4 and ss5,ss6
        assertEquals(4, metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATE_OPERATIONS));
        assertEquals(4, metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATED_RS));

        List<String> rsReportLines = Files.readAllLines(this.rsReportFile.toPath());
        assertEquals(2, rsReportLines.size());
        // RS ID created for a given hash in not deterministic during the split.
        // So, use regex to check for just the hashes in the RS report.
        assertTrue(rsReportLines.stream()
                                .anyMatch(line -> line.matches(".*\t717DE940ADEB9E9DCAF09600065BAAF9A91EE713")));
        assertTrue(rsReportLines.stream()
                                .anyMatch(line -> line.matches(".*\t0304A4760D6365E3BAA8E203AF0355248C05979D")));
    }

    @Test
    // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=231697699
    public void testSplitsInvolvingSameSSWithSameRS_Scenario1() throws Exception {
        Long rs1Accession = 1L;
        ss1 = createSS(1L, rs1Accession, 100L, "C", "T");
        ss2 = createSS(2L, rs1Accession, 101L, "T", "A");
        // Another SS with the same accession as SS2 but different REF/ALT
        SubmittedVariantEntity ss2_another = createSS(ss2.getAccession(), rs1Accession, 102L,
                                                      "T", "");

        // Create multiple entries in clustered variant collection - one for each distinct loci but with the same accession
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = Stream.of(ss1, ss2, ss2_another)
                                                                       .map(clusteringWriter::toClusteredVariantEntity)
                                                                       .collect(Collectors.toList());

        mongoTemplate.insert(Arrays.asList(ss1, ss2, ss2_another), DbsnpSubmittedVariantEntity.class);
        mongoTemplate.insert(multipleEntriesWithSameRS, DbsnpClusteredVariantEntity.class);
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                            ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                            Stream.of(ss1, ss2, ss2_another).map(SubmittedVariantInactiveEntity::new)
                                  .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));

        // Even though both SS2 and SS2_another happen to have the same ID and the same RS but different locus
        // (can happen, see https://www.ebi.ac.uk/panda/jira/browse/EVA-2630),
        // a split operation should assign different RS IDs per SS ID/hash combination.
        // Here SS1-rsLocus1 gets to keep the old RS ID because SS1 is the older SS ID
        // SS2 and SS2_another get two different newly issued RS IDs respectively even though they have the same accession
        Long newRSForSS1 = submittedVariantAccessioningService.get(Collections.singletonList(ss1)).get(0).getData()
                                                              .getClusteredVariantAccession();
        Long newRSForSS2 = submittedVariantAccessioningService.get(Collections.singletonList(ss2)).get(0).getData()
                                                              .getClusteredVariantAccession();
        Long newRSForSS2_another = submittedVariantAccessioningService.get(Collections.singletonList(ss2_another))
                                                                      .get(0)
                                                                      .getData()
                                                                      .getClusteredVariantAccession();
        assertEquals(rs1Accession, newRSForSS1);
        assertNotEquals(rs1Accession, newRSForSS2);
        assertNotEquals(rs1Accession, newRSForSS2_another);
        assertNotEquals(newRSForSS2, newRSForSS2_another);

        List<String> rsReportLines = Files.readAllLines(this.rsReportFile.toPath());
        assertEquals(2, rsReportLines.size());
        // RS ID created for a given hash in not deterministic during the split.
        // So, use regex to check for just the hashes in the RS report.
        assertTrue(rsReportLines.stream()
                                .anyMatch(line -> line.matches(".*\t15F833E1CD27F19EEC66BE89E96B89C99D46A5AB")));
        assertTrue(rsReportLines.stream()
                                .anyMatch(line -> line.matches(".*\t0304A4760D6365E3BAA8E203AF0355248C05979D")));
    }

    @Test
    // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1639249574
    public void testSplitsInvolvingSameSSWithSameRS_Scenario2() throws Exception {
        Long rs1Accession = 1L;
        ss1 = createSS(1L, rs1Accession, 100L, "C", "T");
        // Another SS with the same accession as SS1 but different REF/ALT
        SubmittedVariantEntity ss1_another = createSS(ss1.getAccession(), rs1Accession, 101L,
                                                      "T", "");
        SubmittedVariantOperationEntity splitOperation =
                createScenario2_SplitsInvolvingSameSSWithSameRS(ss1, ss1_another, rs1Accession);
        rsSplitWriter.write(Collections.singletonList(splitOperation));


        // Even though the number of supporting variants for rsLocus2 (ss1) and rsLocus3 (ss1_another) are same
        // and the supporting SS IDs are the same,
        // rsLocus2 gets to keep rs1 because the lexicographical representation of rsLocus2
        // takes priority over that of rsLocus3.
        // RS locus representation of SS1 - 1_chr1_100_SNV
        // RS locus representation of SS1_another - 1_chr1_101_DEL
        // See ClusteredVariantSplittingPolicy.java
        Long newRSForSS1 = submittedVariantAccessioningService.get(Collections.singletonList(ss1)).get(0).getData()
                                                              .getClusteredVariantAccession();
        Long newRSForSS1_another = submittedVariantAccessioningService.get(Collections.singletonList(ss1_another))
                                                                      .get(0).getData()
                                                                      .getClusteredVariantAccession();
        assertEquals(rs1Accession, newRSForSS1);
        assertNotEquals(rs1Accession, newRSForSS1_another);

        List<String> rsReportLines = Files.readAllLines(this.rsReportFile.toPath());
        assertEquals(1, rsReportLines.size());
        // RS ID created for a given hash in not deterministic during the split.
        // So, use regex to check for just the hashes in the RS report.
        assertTrue(rsReportLines.get(0).matches(".*\t38B381F33CE89A941BABDB854FABF267C99843C6"));
    }

    private SubmittedVariantOperationEntity createScenario2_SplitsInvolvingSameSSWithSameRS
            (SubmittedVariantEntity ss1, SubmittedVariantEntity ss1_another, Long rs1Accession) {
        // Create multiple entries in clustered variant collection - one for each distinct loci but with the same accession
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = Stream.of(ss1, ss1_another)
                                                                       .map(clusteringWriter::toClusteredVariantEntity)
                                                                       .collect(Collectors.toList());

        mongoTemplate.insert(Arrays.asList(ss1, ss1_another), DbsnpSubmittedVariantEntity.class);
        mongoTemplate.insert(multipleEntriesWithSameRS, DbsnpClusteredVariantEntity.class);
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                            ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                            Stream.of(ss1, ss1_another).map(SubmittedVariantInactiveEntity::new)
                                  .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);

        return splitOperation;
    }

    @Test
    @DirtiesContext
    // Chosen scenario for testing:
    // https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1639249574
    // There is no specific reason for choosing this scenario for testing other than the fact
    // there is a convenience method createScenario2_SplitsInvolvingSameSSWithSameRS above to set up this scenario.
    public void testIdempotentSplitWrites() throws Exception {
        Long rs1Accession = 1L;
        ss1 = createSS(1L, rs1Accession, 100L, "C", "T");
        // Another SS with the same accession as SS1 but different REF/ALT
        SubmittedVariantEntity ss1_another = createSS(ss1.getAccession(), rs1Accession, 101L,
                                                      "T", "");
        SubmittedVariantOperationEntity splitOperation =
                createScenario2_SplitsInvolvingSameSSWithSameRS(ss1, ss1_another, rs1Accession);

        rsSplitWriter.write(Collections.singletonList(splitOperation));
        DatabaseState databaseStateAfterFirstSplitWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        rsSplitWriter.write(Collections.singletonList(splitOperation));
        DatabaseState databaseStateAfterSecondSplitWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        rsSplitWriter.write(Collections.singletonList(splitOperation));
        DatabaseState databaseStateAfterThirdSplitWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);

        assertEquals(databaseStateAfterSecondSplitWrite, databaseStateAfterFirstSplitWrite);
        assertEquals(databaseStateAfterThirdSplitWrite, databaseStateAfterFirstSplitWrite);

        List<String> rsReportLines = Files.readAllLines(this.rsReportFile.toPath());
        assertEquals(1, rsReportLines.size());
        // RS ID created for a given hash in not deterministic during the split.
        // So, use regex to check for just the hashes in the RS report.
        assertTrue(rsReportLines.get(0).matches(".*\t38B381F33CE89A941BABDB854FABF267C99843C6"));
    }

    @Test
    @DirtiesContext
    /**
     * See EVA-3182 and https://docs.google.com/spreadsheets/d/10NqzlcmKF5cVotJtnYhEFYvD_A7P9WsjN2p2ipYFKxE/edit#rangeid=1226157786
     */
    public void testRetainedRSIsNotCreatedWhenHashExists() throws Exception {
        Long sameRSAccessionToBeUsedForDifferentLoci = 1L;
        // Create SS with the same RS with ss1 and ss2 sharing the same locus (chr + start + type)
        ss1 = createSS(1L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "G");
        ss2 = createSS(2L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "T");

        mongoTemplate.insert(Arrays.asList(ss1, ss2), DbsnpSubmittedVariantEntity.class);
        // Create a separate entry in CVE with the same hash as the RS locus of ss1
        ClusteredVariantEntity ss1CVE = clusteringWriter.toClusteredVariantEntity(ss1);
        ClusteredVariantEntity newEntryInCVE = new ClusteredVariantEntity(7L, ss1CVE.getHashedMessage(), ss1CVE);
        mongoTemplate.save(newEntryInCVE);

        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                Stream.of(ss1, ss2).map(SubmittedVariantInactiveEntity::new).collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));

        // Ensure that SS1 gets to retain the old RS
        List<AccessionWrapper<ISubmittedVariant, String, Long>> ssAssociatedWithRS1 =
                submittedVariantAccessioningService.getByClusteredVariantAccessionIn(
                        Collections.singletonList(sameRSAccessionToBeUsedForDifferentLoci));
        assertEquals(1, ssAssociatedWithRS1.size());
        assertTrue(ssAssociatedWithRS1.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet())
                .containsAll(Arrays.asList(ss1.getAccession())));

        // Ensure that the RS with accession 1 associated with ss1 is NOT created
        // because an RS with accession 7 already has the same hash
        assertTrue(mongoTemplate.findAll(DbsnpClusteredVariantEntity.class).isEmpty());
    }
}