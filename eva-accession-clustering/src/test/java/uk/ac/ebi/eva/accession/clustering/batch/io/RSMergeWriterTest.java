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
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.clustering.test.DatabaseState;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.mongodb.readers.MongoDbCursorItemReader;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_CANDIDATES_READER;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:merge-split-test.properties")
@ContextConfiguration(classes = {RSMergeAndSplitCandidatesReaderConfiguration.class, RSMergeAndSplitWriterConfiguration.class,
        MongoTestConfiguration.class, BatchTestConfiguration.class})
public class RSMergeWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final String DBSNP_SUBMITTED_VARIANT_COLLECTION = "dbsnpSubmittedVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    private static final String SUBMITTED_VARIANT_OPERATION_COLLECTION = "submittedVariantOperationEntity";

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(RS_MERGE_CANDIDATES_READER)
    private MongoDbCursorItemReader<SubmittedVariantOperationEntity> rsMergeCandidatesReader;

    @Autowired
    @Qualifier(RS_SPLIT_CANDIDATES_READER)
    private MongoDbCursorItemReader<SubmittedVariantOperationEntity> rsSplitCandidatesReader;

    @Autowired
    @Qualifier(RS_MERGE_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsMergeWriter;

    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriter;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private SubmittedVariantEntity ss1, ss2, ss3, ss4, ss5, ss6, ss7, ss8, ss9;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    private ClusteredVariantOperationRepository clusteredVariantOperationRepository;

    @Autowired
    private SubmittedVariantOperationRepository submittedVariantOperationRepository;

    @Autowired
    private MetricCompute metricCompute;

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate) {

        return createSS(ssAccession, rsAccession, start, reference, alternate, ASSEMBLY);
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate, String assemblyToUse) {

        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession + assemblyToUse, assemblyToUse, 60711,
                                          "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false, false,
                                          false, 1);
    }

    private void createMergeAndSplitCandidateEntries() {
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 4    4   chr1/100/SNV
         * 2    2   chr1/103/SNV
         * 5    5   chr1/103/SNV
         * 6    4   chr1/104/SNV
         * 7    4   chr1/105/SNV
         * 8    2   chr1/106/SNV
         * 9    5   chr1/107/SNV
         * RS1, RS4 and RS2, RS5 are marked as merge candidate entries since these RS pairs share the same locus
         * Following are split candidate entries since these SS share the same RS but differ in loci:
         *      SS2,SS8
         *      SS4,SS6,SS7
         *      SS5,SS9
         *
         */

        //ss1 will be inserted to dbsnpSubmittedVariantEntity and ss4 to submittedVariantEntity collections respectively
        ss1 = createSS(1L, 1L, 100L, "C", "T");
        ss4 = createSS(4L, 4L, 100L, "C", "A");
        //Candidates for merge are entries with same locus but different RS
        SubmittedVariantInactiveEntity ssInactive1 = new SubmittedVariantInactiveEntity(ss1);
        SubmittedVariantInactiveEntity ssInactive4 = new SubmittedVariantInactiveEntity(ss4);
        SubmittedVariantOperationEntity mergeOperation1 = new SubmittedVariantOperationEntity();
        mergeOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                             ss1.getAccession(), null, "Different RS with matching loci",
                             Arrays.asList(ssInactive1, ssInactive4));
        mergeOperation1.setId(ClusteringWriter.getMergeCandidateId(mergeOperation1));

        //ss2 will be inserted to dbsnpSubmittedVariantEntity and ss5 to submittedVariantEntity collections respectively
        ss2 = createSS(2L, 2L, 103L, "A", "C");
        ss5 = createSS(5L, 5L, 103L, "A", "T");
        //Candidates for merge are entries with same locus but different RS
        SubmittedVariantInactiveEntity ssInactive2 = new SubmittedVariantInactiveEntity(ss2);
        SubmittedVariantInactiveEntity ssInactive5 = new SubmittedVariantInactiveEntity(ss5);
        SubmittedVariantOperationEntity mergeOperation2 = new SubmittedVariantOperationEntity();
        mergeOperation2.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                             ss2.getAccession(),
                             null, "Different RS with matching loci",
                             Arrays.asList(ssInactive2, ssInactive5));
        mergeOperation2.setId(ClusteringWriter.getMergeCandidateId(mergeOperation2));

        //Candidates for split are entries with same RS but different locus
        ss6 = createSS(6L, 4L, 104L, "G", "A");
        ss7 = createSS(7L, 4L, 105L, "T", "G");
        ss8 = createSS(8L, 2L, 106L, "A", "G");
        ss9 = createSS(9L, 5L, 107L, "C", "T");
        SubmittedVariantInactiveEntity ssInactive6 = new SubmittedVariantInactiveEntity(ss6);
        SubmittedVariantInactiveEntity ssInactive7 = new SubmittedVariantInactiveEntity(ss7);
        SubmittedVariantInactiveEntity ssInactive8 = new SubmittedVariantInactiveEntity(ss8);
        SubmittedVariantInactiveEntity ssInactive9 = new SubmittedVariantInactiveEntity(ss9);

        SubmittedVariantOperationEntity splitOperation1 = new SubmittedVariantOperationEntity();
        SubmittedVariantOperationEntity splitOperation2 = new SubmittedVariantOperationEntity();
        SubmittedVariantOperationEntity splitOperation3 = new SubmittedVariantOperationEntity();
        splitOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                             ss2.getAccession(), "Hash mismatch with " + ss2.getClusteredVariantAccession(),
                             Arrays.asList(ssInactive2, ssInactive8));
        splitOperation1.setId(ClusteringWriter.getSplitCandidateId(splitOperation1));
        splitOperation2.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                             ss4.getAccession(), "Hash mismatch with " + ss4.getClusteredVariantAccession(),
                             Arrays.asList(ssInactive4, ssInactive6, ssInactive7));
        splitOperation2.setId(ClusteringWriter.getSplitCandidateId(splitOperation2));
        splitOperation3.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                             ss5.getAccession(), "Hash mismatch with " + ss5.getClusteredVariantAccession(),
                             Arrays.asList(ssInactive5, ssInactive9));
        splitOperation3.setId(ClusteringWriter.getSplitCandidateId(splitOperation3));

        List<SubmittedVariantEntity> ssToInsertToDbsnpSVE = Arrays.asList(ss1, ss2);
        List<SubmittedVariantEntity> ssToInsertToSVE = Arrays.asList(ss4, ss5, ss6, ss7, ss8, ss9);

        mongoTemplate.insert(ssToInsertToDbsnpSVE, DBSNP_SUBMITTED_VARIANT_COLLECTION);
        mongoTemplate.insert(ssToInsertToSVE, SUBMITTED_VARIANT_COLLECTION);
        mongoTemplate.insert(Arrays.asList(mergeOperation1, mergeOperation2,
                                           splitOperation1, splitOperation2, splitOperation3),
                             SUBMITTED_VARIANT_OPERATION_COLLECTION);
    }

    private void cleanup() {
        mongoClient.dropDatabase(TEST_DB);
        metricCompute.clearCount();
    }

    @Before
    public void setUp() {
        cleanup();
    }

    @After
    public void tearDown() {
        cleanup();
    }

    private void assertRSAssociatedWithSS(Long rsAccession, SubmittedVariantEntity submittedVariantEntity)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        assertEquals(rsAccession,
                     submittedVariantAccessioningService
                             .getAllActiveByAssemblyAndAccessionIn(ASSEMBLY,
                                                                   Collections.singletonList(
                                                                           submittedVariantEntity.getAccession()))
                             .stream().findFirst().get().getData().getClusteredVariantAccession());
    }

    @Test
    @DirtiesContext
    public void testWriteRSMerges() throws Exception {
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 4    4   chr1/100/SNV
         * 2    2   chr1/103/SNV
         * 5    5   chr1/103/SNV
         * 6    4   chr1/104/SNV
         * 7    4   chr1/105/SNV
         * 8    2   chr1/106/SNV
         * 9    5   chr1/107/SNV
         * RS1, RS4 and RS2, RS5 are marked as merge candidate entries since these RS pairs share the same locus
         * Following are split candidate entries since these SS share the same RS but differ in loci:
         *      SS2,SS8
         *      SS4,SS6,SS7
         *      SS5,SS9
         *
         */
        createMergeAndSplitCandidateEntries();
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity submittedVariantOperationEntity;
        rsMergeCandidatesReader.open(new ExecutionContext());
        while ((submittedVariantOperationEntity = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(submittedVariantOperationEntity);
        }

        //Perform merge
        rsMergeWriter.write(submittedVariantOperationEntities);

        //Ensure operations collections have appropriate entries to reflect rs4 merged to rs1 & rs5 merged to rs2
        assertEquals(Long.valueOf(1L), clusteredVariantOperationRepository.findAllByAccession(4L).get(0)
                                                                          .getMergedInto());
        assertEquals(Long.valueOf(2L), clusteredVariantOperationRepository.findAllByAccession(5L).get(0)
                                                                          .getMergedInto());
        assertEquals ("Original rs4 associated with SS was merged into rs1.",
                     submittedVariantOperationRepository.findAllByAccession(4L).get(0).getReason());
        assertEquals("Original rs5 associated with SS was merged into rs2.",
                     submittedVariantOperationRepository.findAllByAccession(5L).get(0).getReason());

        // Ensure that RS1 and RS2 are present in the clustered variant collection post merge
        assertEquals(Long.valueOf(1L), clusteredVariantAccessioningService.getByAccession(1L).getAccession());
        assertEquals(Long.valueOf(2L), clusteredVariantAccessioningService.getByAccession(2L).getAccession());

        assertThrows(AccessionMergedException.class, () -> {clusteredVariantAccessioningService.getByAccession(4L);});
        assertThrows(AccessionMergedException.class, () -> {clusteredVariantAccessioningService.getByAccession(5L);});

        //After merge SS-RS associations: rs4 merged to rs1 & rs5 merged to rs2
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 4    1   chr1/100/SNV
         * 2    2   chr1/103/SNV
         * 5    2   chr1/103/SNV
         * 6    1   chr1/104/SNV
         * 7    1   chr1/105/SNV
         * 8    2   chr1/106/SNV
         * 9    2   chr1/107/SNV
         */
        assertRSAssociatedWithSS(1L, ss1);
        assertRSAssociatedWithSS(2L, ss2);
        assertRSAssociatedWithSS(1L, ss4);
        assertRSAssociatedWithSS(2L, ss5);
        assertRSAssociatedWithSS(1L, ss6);
        assertRSAssociatedWithSS(1L, ss7);
        assertRSAssociatedWithSS(2L, ss8);
        assertRSAssociatedWithSS(2L, ss9);

        // After rs4 merge to rs1 and rs5 merge to rs2
        // split candidates operations involving rs4 and rs5 should no longer be present
        assertFalse(submittedVariantOperationRepository
                        .findAllByAccession(4L).stream()
                        .anyMatch(event -> event.getEventType().equals(
                                RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE)));
        assertFalse(submittedVariantOperationRepository
                            .findAllByAccession(5L).stream()
                            .anyMatch(event -> event.getEventType().equals(
                                    RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE)));
        // Post-merge SS1, SS4, SS6 and SS7 all share rs1 but have different locus
        // and hence generate one split candidate event
        // Post-merge SS2, SS5, SS8 and SS9 all share rs2 but have different locus
        // and hence generate a split candidate event
        List<SubmittedVariantOperationEntity> splitEvents = new ArrayList<>();
        SubmittedVariantOperationEntity tempObj;
        rsSplitCandidatesReader.open(new ExecutionContext());
        while ((tempObj = rsSplitCandidatesReader.read()) != null) {
                splitEvents.add(tempObj);
        }
        assertSplitCandidateEvents(1L, Arrays.asList(1L, 4L, 6L, 7L), splitEvents);
        assertSplitCandidateEvents(2L, Arrays.asList(2L, 5L, 8L, 9L), splitEvents);

        /* Check clustering counts **/
        // Only merge destinations RS1 and RS2 will be created
        // The other two RSs RS4 and RS5 were identified as mergees and hence won't be created if not already present
        assertEquals(2, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_CREATED));
        assertEquals(2, metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_MERGE_OPERATIONS));
        // 5 SS IDs were updated due to RS4 -> RS1 merge and RS5 -> RS2 merge
        assertEquals(5, metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATED_RS));
        assertEquals(5, metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATE_OPERATIONS));
    }

    // Check if a given RS has the participating SS listed in the split candidates event
    private void assertSplitCandidateEvents(Long rsAccession, List<Long> expectedParticipatingSS,
                                            List<SubmittedVariantOperationEntity> splitEvents) throws Exception {
        splitEvents = splitEvents.stream().filter(e -> e.getInactiveObjects().get(0)
                                                        .getClusteredVariantAccession().equals(rsAccession))
                                 .collect(Collectors.toList());
        assertEquals(1, splitEvents.size());
        SubmittedVariantOperationEntity splitEvent = splitEvents.get(0);
        assertEquals("Hash mismatch with " + rsAccession, splitEvent.getReason());
        List<SubmittedVariantInactiveEntity> participatingSSInSplitEvent = splitEvent.getInactiveObjects();

        assertEquals(expectedParticipatingSS.size(), participatingSSInSplitEvent.size());
        assertTrue(participatingSSInSplitEvent.stream()
                                              .map(InactiveSubDocument::getAccession)
                                              .collect(Collectors.toSet())
                                              .containsAll(expectedParticipatingSS));
        // Ensure that all the inactive objects have the same RS
        assertFalse(participatingSSInSplitEvent.stream()
                                               .anyMatch(e -> !e.getClusteredVariantAccession().equals(rsAccession)));
    }

    @Test
    @DirtiesContext
    public void testMultiLevelRSMerges() throws Exception {
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 2    2   chr1/100/SNV
         * 3    2   chr1/103/SNV
         * 4    3   chr1/103/SNV
         * SS1/RS1 and SS2/RS2, SS3/RS2 and SS4/RS3 are marked as merge candidate entries since these RS pairs share the same locus
         * SS2/RS2 and SS3/RS2 are split candidate entries because the same RS has different loci
         */
        createMultiLevelMergeScenario();
        // Create similar entries as above in another assembly
        createSimilarEntriesInAnotherAssembly();
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity temp;
        rsMergeCandidatesReader.open(new ExecutionContext());
        while ((temp = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(temp);
        }

        //Perform merge
        rsMergeWriter.write(submittedVariantOperationEntities);

        //After merge SS-RS associations: rs2 merged to rs1, Merge target for rs3 inferred as: rs3 -> rs2 -> rs1
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 2    1   chr1/100/SNV
         * 3    1   chr1/103/SNV
         * 4    1   chr1/103/SNV
         *
         */
        // Check rs2 to rs1 merge
        assertMergeOp(ss2.getClusteredVariantAccession(), ss1.getClusteredVariantAccession(), ASSEMBLY);
        // Check rs3 to rs1 merge
        assertMergeOp(ss3.getClusteredVariantAccession(), ss1.getClusteredVariantAccession(), ASSEMBLY);
        assertRSAssociatedWithSS(1L, ss1);
        assertRSAssociatedWithSS(1L, ss2);
        assertRSAssociatedWithSS(1L, ss3);
        assertRSAssociatedWithSS(1L, ss4);
    }

    @Test
    @DirtiesContext
    public void testRSWithSameHashAsMergeDestinationAlreadyExistsInDBWithHigherAccession() throws Exception {
        ss1 = createSS(1L, 1L, 100L, "C", "T");
        ss2 = createSS(2L, 2L, 100L, "C", "A");
        ss3 = createSS(3L, 3L, 100L, "A", "G");
        this.mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DBSNP_SUBMITTED_VARIANT_COLLECTION);

        DbsnpClusteredVariantEntity rs2 = this.createRS(ss2);
        this.mongoTemplate.insert(rs2);

        SubmittedVariantOperationEntity mergeOperation1 = new SubmittedVariantOperationEntity();
        mergeOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                ss1.getAccession(), null, "Different RS with matching loci",
                Stream.of(ss1, ss2, ss3).map(SubmittedVariantInactiveEntity::new).collect(Collectors.toList()));
        mergeOperation1.setId(ClusteringWriter.getMergeCandidateId(mergeOperation1));
        this.mongoTemplate.insert(Arrays.asList(mergeOperation1), SUBMITTED_VARIANT_OPERATION_COLLECTION);

        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity temp;
        rsMergeCandidatesReader.open(new ExecutionContext());
        while ((temp = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(temp);
        }

        //Perform merge
        rsMergeWriter.write(submittedVariantOperationEntities);

        // Check rs2,rs3 merged into to rs1
        assertMergeOp(ss2.getClusteredVariantAccession(), ss1.getClusteredVariantAccession(), ASSEMBLY);
        assertMergeOp(ss3.getClusteredVariantAccession(), ss1.getClusteredVariantAccession(), ASSEMBLY);

        // Check rs1 to rs3 all have same rs
        assertRSAssociatedWithSS(1L, ss1);
        assertRSAssociatedWithSS(1L, ss2);
        assertRSAssociatedWithSS(1L, ss3);
    }

    @Test
    @DirtiesContext
    public void testRSWithSameHashAsMergeDestinationAlreadyExistsInDBWithLowerAccession() throws Exception {
        ss1 = createSS(1L, 1L, 100L, "C", "T");
        ss2 = createSS(2L, 2L, 100L, "C", "A");
        ss3 = createSS(3L, 3L, 100L, "A", "G");
        this.mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DBSNP_SUBMITTED_VARIANT_COLLECTION);

        DbsnpClusteredVariantEntity rs1 = this.createRS(ss1);
        this.mongoTemplate.insert(rs1);

        SubmittedVariantOperationEntity mergeOperation1 = new SubmittedVariantOperationEntity();
        mergeOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                ss1.getAccession(), null, "Different RS with matching loci",
                Stream.of(ss1, ss2, ss3).map(SubmittedVariantInactiveEntity::new).collect(Collectors.toList()));
        mergeOperation1.setId(ClusteringWriter.getMergeCandidateId(mergeOperation1));
        this.mongoTemplate.insert(Arrays.asList(mergeOperation1), SUBMITTED_VARIANT_OPERATION_COLLECTION);

        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity temp;
        rsMergeCandidatesReader.open(new ExecutionContext());
        while ((temp = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(temp);
        }

        //Perform merge
        rsMergeWriter.write(submittedVariantOperationEntities);

        // Check rs2,rs3 merged into to rs1
        assertMergeOp(ss2.getClusteredVariantAccession(), ss1.getClusteredVariantAccession(), ASSEMBLY);
        assertMergeOp(ss3.getClusteredVariantAccession(), ss1.getClusteredVariantAccession(), ASSEMBLY);

        // Check rs1 to rs3 all have same rs
        assertRSAssociatedWithSS(1L, ss1);
        assertRSAssociatedWithSS(1L, ss2);
        assertRSAssociatedWithSS(1L, ss3);
    }

    private void assertMergeOp(Long mergee, Long merger, String assemblyToUse) {
        assertEquals(0, this.clusteredVariantAccessioningService
                .getAllActiveByAssemblyAndAccessionIn(assemblyToUse, Arrays.asList(mergee)).size());
        assertEquals(1, this.mongoTemplate.find(Query.query(Criteria.where("inactiveObjects.asm")
                                                                    .is(assemblyToUse).and("accession").is(mergee)
                                                                    .and("mergeInto").is(merger)),
                                                DbsnpClusteredVariantOperationEntity.class).size());
    }

    public void createMultiLevelMergeScenario() {
        //ss1 will be inserted to dbsnpSubmittedVariantEntity and ss4 to submittedVariantEntity collections respectively
        ss1 = createSS(1L, 1L, 100L, "C", "T");
        ss2 = createSS(2L, 2L, 100L, "C", "A");
        ss3 = createSS(3L, 2L, 103L, "A", "G");
        ss4 = createSS(4L, 3L, 103L, "T", "C");
        this.mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DBSNP_SUBMITTED_VARIANT_COLLECTION);
        this.mongoTemplate.insert(Collections.singletonList(ss4), SUBMITTED_VARIANT_COLLECTION);

        SubmittedVariantOperationEntity mergeOperation1 = new SubmittedVariantOperationEntity();
        mergeOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                             ss1.getAccession(), null, "Different RS with matching loci",
                             Stream.of(ss1, ss2).map(SubmittedVariantInactiveEntity::new).collect(Collectors.toList()));
        mergeOperation1.setId(ClusteringWriter.getMergeCandidateId(mergeOperation1));
        SubmittedVariantOperationEntity mergeOperation2 = new SubmittedVariantOperationEntity();
        mergeOperation2.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                             ss3.getAccession(), null, "Different RS with matching loci",
                             Stream.of(ss3, ss4).map(SubmittedVariantInactiveEntity::new).collect(Collectors.toList()));
        mergeOperation2.setId(ClusteringWriter.getMergeCandidateId(mergeOperation2));

        SubmittedVariantOperationEntity splitOperation1 = new SubmittedVariantOperationEntity();
        splitOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                             ss2.getAccession(), null, "Hash mismatch with " + ss2.getClusteredVariantAccession(),
                             Stream.of(ss2, ss3).map(SubmittedVariantInactiveEntity::new).collect(Collectors.toList()));
        splitOperation1.setId(ClusteringWriter.getSplitCandidateId(splitOperation1));

        this.mongoTemplate.insert(Arrays.asList(mergeOperation1, mergeOperation2, splitOperation1),
                                  SUBMITTED_VARIANT_OPERATION_COLLECTION);
    }

    private void createSimilarEntriesInAnotherAssembly() {
        //ss1-ss3 will be inserted to dbsnpSubmittedVariantEntity
        // and ss4 to submittedVariantEntity collections respectively
        SubmittedVariantEntity ss1_another_asm = createSS(1L, 1L, 100L, "C", "T", "ASM_ANOTHER");
        SubmittedVariantEntity ss2_another_asm = createSS(2L, 1L, 100L, "C", "A", "ASM_ANOTHER");
        SubmittedVariantEntity ss3_another_asm = createSS(3L, 2L, 103L, "A", "G", "ASM_ANOTHER");
        SubmittedVariantEntity ss4_another_asm = createSS(4L, 3L, 103L, "T", "C", "ASM_ANOTHER");
        this.mongoTemplate.insert(Arrays.asList(ss1_another_asm, ss2_another_asm, ss3_another_asm),
                                  DBSNP_SUBMITTED_VARIANT_COLLECTION);
        this.mongoTemplate.insert(Collections.singletonList(ss4_another_asm), SUBMITTED_VARIANT_COLLECTION);

        DbsnpClusteredVariantEntity rs1_another_asm = this.createRS(ss1_another_asm);
        // create operations for rs2 -> rs1 merge to test EVA-2974 root cause
        // see https://www.ebi.ac.uk/panda/jira/browse/EVA-2974?focusedCommentId=405919&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-405919
        DbsnpClusteredVariantOperationEntity rs2ToRs1MergeOp = new DbsnpClusteredVariantOperationEntity();
        rs2ToRs1MergeOp.fill(EventType.MERGED, 2L, 1L,
                             "Identical clustered variant received multiple RS identifiers",
                             Arrays.asList(new DbsnpClusteredVariantInactiveEntity(rs1_another_asm)));
        this.mongoTemplate.save(rs2ToRs1MergeOp);
    }

    @Test
    @DirtiesContext
    public void testIdempotentMergeWrites() throws Exception {
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 2    2   chr1/100/SNV
         * 3    2   chr1/103/SNV
         * 4    3   chr1/103/SNV
         * SS1/RS1 and SS2/RS2, SS3/RS2 and SS4/RS3 are marked as merge candidate entries since these RS pairs share the same locus
         * SS2/RS2 and SS3/RS2 are split candidate entries because the same RS has different loci
         */
        createMultiLevelMergeScenario();
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity temp;
        rsMergeCandidatesReader.open(new ExecutionContext());
        while ((temp = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(temp);
        }

        //First run of RS Merges
        rsMergeWriter.write(submittedVariantOperationEntities);
        DatabaseState databaseStateAfterFirstMergeWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        rsMergeWriter.write(submittedVariantOperationEntities);
        DatabaseState databaseStateAfterSecondMergeWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        rsMergeWriter.write(submittedVariantOperationEntities);
        DatabaseState databaseStateAfterThirdMergeWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);

        assertEquals(databaseStateAfterSecondMergeWrite, databaseStateAfterFirstMergeWrite);
        assertEquals(databaseStateAfterThirdMergeWrite, databaseStateAfterFirstMergeWrite);
    }

    private DbsnpClusteredVariantEntity createRS(SubmittedVariantEntity sve) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant cv = new ClusteredVariant(sve.getReferenceSequenceAccession(), sve.getTaxonomyAccession(),
                                                   sve.getContig(),
                                                   sve.getStart(),
                                                   new Variant(sve.getContig(), sve.getStart(), sve.getStart(),
                                                               sve.getReferenceAllele(),
                                                               sve.getAlternateAllele()).getType(),
                                                   true, null);
        String hash = hashingFunction.apply(cv);
        return new DbsnpClusteredVariantEntity(sve.getClusteredVariantAccession(), hash, cv);
    }
}
