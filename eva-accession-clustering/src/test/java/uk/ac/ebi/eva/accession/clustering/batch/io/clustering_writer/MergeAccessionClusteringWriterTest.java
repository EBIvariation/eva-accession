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
 */
package uk.ac.ebi.eva.accession.clustering.batch.io.clustering_writer;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.assertj.core.util.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReader;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.batch.io.MongoDbCursorItemReader;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.batch.io.clustering_writer.ClusteringAssertions.assertClusteringCounts;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertAccessionEqual;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertAssemblyAccessionEqual;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertClusteredVariantAccessionEqual;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertReferenceSequenceAccessionEqual;

/**
 * This class handles some scenarios of ClusteringWriter where redundant RSs are discovered and merged.
 *
 * That includes all the scenarios that involve writing to the Operation collections, e.g.
 * clusteredVariantOperationEntity to register an RS merge.
 * Other test classes in this folder take care of other scenarios.
 */
@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class,
        SubmittedVariantAccessioningConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-writer-test.properties")
public class MergeAccessionClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    public static final String DBSNP_CLUSTERED_VARIANT_COLLECTION = "dbsnpClusteredVariantEntity";

    public static final String DBSNP_SUBMITTED_VARIANT_COLLECTION = "dbsnpSubmittedVariantEntity";

    public static final long EVA_CLUSTERED_VARIANT_RANGE_START = 3000000000L;

    public static final long EVA_SUBMITTED_VARIANT_RANGE_START = 5000000000L;

    public static final long START = 100L;

    public static final String NOT_REMAPPED = null;

    private static final String ASM_1 = "asm1";

    private static final String ASM_2 = "asm2";

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MetricCompute metricCompute;

    // Current clustering sequence is:
    // generate merge split candidates from clustered variants -> perform merge
    // -> perform split -> cluster new variants
    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriterPreMergeAndSplit;

    @Autowired
    @Qualifier(NON_CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriterPostMergeAndSplit;

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
    @Qualifier(RS_SPLIT_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsSplitWriter;

    @Autowired
    @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES)
    private ItemWriter clearRSMergeAndSplitCandidates;

    @Autowired
    private Long accessioningMonotonicInitSs;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    private Function<ISubmittedVariant, String> hashingFunction;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private static final String defaultReferenceAlleleForTesting = "T";

    private static final String defaultAlternateAlleleForTesting = "A";

    @Before
    public void setUp() {
        mongoTemplate.getDb().drop();
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
        metricCompute.clearCount();
    }

    @Test
    @DirtiesContext
    public void merge_eva_clustered_accession() throws Exception {
        Long rs1 = 3000000000L;
        Long rs2 = 3100000000L;
        long ssToRemap = 5000000000L;
        long ss2 = 5100000000L;
        mergeClusteredAccession(rs1, rs2, ssToRemap, ss2, 0, 2, 0, 1, 0, 2, 0, 1);
        // NOTE: the clustered variant record for RS1 in ASM2 will be created during merge (accounted for in expectedClusteredVariantsCreated)
        // and the existing RS2 record in ASM2 will be removed (accounted for in expectedClusteredVariantsUpdated)
        assertClusteringCounts(metricCompute, 1, 1, 1, 0, 0, 1, 1);
        assertMergedInto(rs1, rs2, ss2);
    }

    @Test
    @DirtiesContext
    public void merge_eva_clustered_accession_reversed() throws Exception {
        Long rs1 = 3100000000L;
        Long rs2 = 3000000000L;
        Long ssToRemap = 5000000000L;
        Long ss2 = 5100000000L;
        mergeClusteredAccession(rs1, rs2, ssToRemap, ss2, 0, 2, 0, 1, 0, 2, 0, 1);
        // No new clustering variant record will be created or no existing RS will be updated
        // since the remapped submitted variant is clustered into an existing RS (rs2)
        // Merge is recorded only in the clustered and submitted operations tables
        // ssToRemap being in EVA SNP collection will be clustered and the relevant update operation will be created
        // Hence expectedSubmittedVariantsUpdatedRs and expectedSubmittedVariantOperationsWritten will be 1 each
        assertClusteringCounts(metricCompute, 0, 0, 1, 0, 0, 1, 1);
        assertMergedInto(rs2, rs1, ssToRemap);
    }

    @Test
    @DirtiesContext
    public void merge_dbsnp_clustered_accession() throws Exception {
        Long rs1 = 30L;
        Long rs2 = 31L;
        Long ssToRemap = 50L;
        Long ss2 = 51L;
        mergeClusteredAccession(rs1, rs2, ssToRemap, ss2, 2, 0, 1, 0, 2, 0, 1, 0);
        // NOTE: the clustered variant record for RS1 in ASM2 will be created during merge (accounted for in expectedClusteredVariantsCreated)
        // and the existing RS2 record in ASM2 will be removed (accounted for in expectedClusteredVariantsUpdated)
        // Existing SS2 will be updated (accounted for in expectedSubmittedVariantsUpdatedRs and expectedSubmittedVariantOperationsWritten)
        assertClusteringCounts(metricCompute, 1, 1, 1, 0, 0, 1, 1);
        assertMergedInto(rs1, rs2, ss2);
    }

    @Test
    @DirtiesContext
    public void merge_eva_into_dbsnp_clustered_accession() throws Exception {
        Long rs1 = 3000000000L;
        Long rs2 = 31L;
        Long ssToRemap = 5000000000L;
        Long ss2 = 51L;
        mergeClusteredAccession(rs1, rs2, ssToRemap, ss2, 1, 1, 0, 1, 1, 1, 0, 1);
        // No new clustering variant record will be created or no existing RS will be updated
        // since the remapped submitted variant is clustered into an existing RS (rs2)
        // Merge is recorded only in the clustered and submitted operations tables
        // ssToRemap being in EVA SNP collection will be clustered and the relevant update operation will be created
        // Hence expectedSubmittedVariantsUpdatedRs and expectedSubmittedVariantOperationsWritten will be 1 each
        assertClusteringCounts(metricCompute, 0, 0, 1, 0, 0, 1, 1);
        assertMergedInto(rs2, rs1, ssToRemap);
    }

    @Test
    @DirtiesContext
    public void do_not_merge_dbsnp_into_eva_clustered_accession() throws Exception {
        Long rs1 = 30L;
        Long rs2 = 3100000000L;
        Long ssToRemap = 50L;
        Long ss2 = 5100000000L;
        // rs2 is removed from CVE post-merge and rs1 is created in dbSnpCVE post-merge (hence the 1 and -1 in the last 2 arguments)
        mergeClusteredAccession(rs1, rs2, ssToRemap, ss2, 2, 0, 0, 1, 1, 1, 0, 1, 1, -1);
        assertClusteringCounts(metricCompute, 1, 1, 1, 0, 0, 1, 1);
        assertMergedInto(rs1, rs2, ss2);
    }

    public void mergeClusteredAccession(Long rs1, Long rs2, Long ssToRemap, Long ss2,
                                        int expectedDbsnpCve, int expectedCve,
                                        int expectedDbsnpCvOperations, int expectedCvOperations,
                                        int expectedDbsnpSve, int expectedSve,
                                        int expectedDbsnpSvOperations, int expectedSvOperations,
                                        int additionalDbsnpCveCreatedPostMerge, int additionalCveCreatedPostMerge)
            throws Exception {
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        mongoTemplate.insert(createClusteredVariantEntity(ASM_1, rs1), getClusteredTable(rs1));

        // rs2 will be merged into rs1, but the document will be kept to describe how rs1 maps into this second assembly
        mongoTemplate.insert(createClusteredVariantEntity(ASM_2, rs2), getClusteredTable(rs2));

        // ssToRemap in the old assembly, will be remapped to asm2 (see sve1Remapped below)
        mongoTemplate.insert(createSubmittedVariantEntity(ASM_1, START, rs1, ssToRemap, NOT_REMAPPED),
                             getSubmittedCollection(ssToRemap));

        // ss2 in the new assembly, will change its RS to rs1 when we realise rs1 and rs2 should be merged
        // because they both map to the same position in asm2
        mongoTemplate.insert(createSubmittedVariantEntity(ASM_2, START, rs2, ss2, NOT_REMAPPED), getSubmittedCollection(ss2));

        assertDatabaseCounts(expectedDbsnpCve - additionalDbsnpCveCreatedPostMerge,
                             expectedCve - additionalCveCreatedPostMerge, 0, 0,
                             expectedDbsnpSve, expectedSve, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(ASM_2, START, rs1, ssToRemap, ASM_1,
                                                                           defaultAlternateAlleleForTesting,
                                                                           defaultReferenceAlleleForTesting);
        // To ensure that we don't create collision with ss2,
        // use alleles other than defaultAlternateAlleleForTesting or defaultReferenceAlleleForTesting
        mongoTemplate.insert(sve1Remapped, getSubmittedCollection(sve1Remapped.getAccession()));
        // Include the insertion above for number of expected SVE or DbsnpSVE
        if (ssToRemap >= accessioningMonotonicInitSs) {
            expectedSve += 1;
        } else {
            expectedDbsnpSve += 1;
        }
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(expectedDbsnpCve, expectedCve, expectedDbsnpCvOperations, expectedCvOperations,
                             expectedDbsnpSve, expectedSve, expectedDbsnpSvOperations, expectedSvOperations);

        assertAssembliesPresent(Sets.newTreeSet(ASM_1, ASM_2));
    }

    public void mergeClusteredAccession(Long rs1, Long rs2, Long ssToRemap, Long ss2,
                                        int expectedDbsnpCve, int expectedCve,
                                        int expectedDbsnpCvOperations, int expectedCvOperations,
                                        int expectedDbsnpSve, int expectedSve,
                                        int expectedDbsnpSvOperations, int expectedSvOperations) throws Exception {
        mergeClusteredAccession(rs1, rs2, ssToRemap, ss2, expectedDbsnpCve, expectedCve, expectedDbsnpCvOperations,
                                expectedCvOperations, expectedDbsnpSve, expectedSve, expectedDbsnpSvOperations,
                                expectedSvOperations, 0, 0);
    }



    private ClusteredVariantEntity createClusteredVariantEntity(String assembly, Long rs) {
        ClusteredVariant cv = new ClusteredVariant(assembly, 1000, "1", 100L, VariantType.SNV, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new ClusteredVariantEntity(rs, cvHash, cv, 1);
    }

    private String getClusteredTable(Long rs1) {
        return isEvaClusteredVariant(rs1) ? CLUSTERED_VARIANT_COLLECTION : DBSNP_CLUSTERED_VARIANT_COLLECTION;
    }

    private boolean isEvaClusteredVariant(Long rs1) {
        return rs1 >= EVA_CLUSTERED_VARIANT_RANGE_START;
    }

    private String getSubmittedCollection(Long ss1) {
        return isEvaSubmittedVariant(ss1) ? SUBMITTED_VARIANT_COLLECTION : DBSNP_SUBMITTED_VARIANT_COLLECTION;
    }

    private boolean isEvaSubmittedVariant(Long ss1) {
        return ss1 >= EVA_SUBMITTED_VARIANT_RANGE_START;
    }

    private void assertDatabaseCounts(int expectedDbsnpCve, int expectedCve,
                                      int expectedDbsnpCvOperations, int expectedCvOperations,
                                      int expectedDbsnpSve, int expectedSve,
                                      int expectedDbsnpSvOperations, int expectedSvOperations) {
        assertEquals(expectedDbsnpCve, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(expectedCve, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(expectedDbsnpCvOperations, mongoTemplate.count(new Query(),
                                                                    DbsnpClusteredVariantOperationEntity.class));
        assertEquals(expectedCvOperations, mongoTemplate.count(new Query(), ClusteredVariantOperationEntity.class));
        assertEquals(expectedDbsnpSve, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(expectedSve, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(expectedDbsnpSvOperations, mongoTemplate.count(new Query(),
                                                                    DbsnpSubmittedVariantOperationEntity.class));
        assertEquals(expectedSvOperations, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
    }

    private void assertMergedInto(Long mergedInto, Long originalAccession, Long... updatedSubmittedVariant) {
        // Confine the variants being inspected to the remapped assembly
        List<SubmittedVariantEntity> submittedVariants =
                mongoTemplate
                .findAll(SubmittedVariantEntity.class)
                .stream().filter(entity -> entity.getReferenceSequenceAccession().equals(ASM_2))
                .collect(Collectors.toList());
        submittedVariants.addAll(mongoTemplate
                                         .findAll(DbsnpSubmittedVariantEntity.class)
                                         .stream()
                                         .filter(entity -> entity.getReferenceSequenceAccession().equals(ASM_2))
                                         .collect(Collectors.toList()));
        assertClusteredVariantAccessionEqual(Sets.newTreeSet(mergedInto), submittedVariants);

        // Confine the variants being inspected to the remapped assembly
        List<ClusteredVariantEntity> clusteredVariants =
                mongoTemplate
                        .findAll(ClusteredVariantEntity.class)
                        .stream().filter(entity -> entity.getAssemblyAccession().equals(ASM_2))
                        .collect(Collectors.toList());
        clusteredVariants.addAll(mongoTemplate
                                         .findAll(DbsnpClusteredVariantEntity.class)
                                         .stream().filter(entity -> entity.getAssemblyAccession().equals(ASM_2))
                                         .collect(Collectors.toList()));
        assertAccessionEqual(Sets.newTreeSet(mergedInto), clusteredVariants);

        EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity> clusteredOp =
                mongoTemplate.findOne(new Query(), ClusteredVariantOperationEntity.class);
        if (clusteredOp == null) {
            clusteredOp = mongoTemplate.findOne(new Query(), DbsnpClusteredVariantOperationEntity.class);
        }
        assertEquals(EventType.MERGED, clusteredOp.getEventType());
        assertEquals(originalAccession, clusteredOp.getAccession());
        assertEquals(mergedInto, clusteredOp.getMergedInto());
        assertNotNull(clusteredOp.getCreatedDate());

        List<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>> submittedOps =
                mongoTemplate.findAll(SubmittedVariantOperationEntity.class);
        if (submittedOps.isEmpty()) {
            submittedOps = mongoTemplate.findAll(DbsnpSubmittedVariantOperationEntity.class);
        }
        assertTrue(submittedOps.stream().map(EventDocument::getEventType).allMatch(EventType.UPDATED::equals));
        assertEquals(Sets.newTreeSet(updatedSubmittedVariant), submittedOps.stream()
                                                                           .map(EventDocument::getAccession)
                                                                           .collect(Collectors.toSet()));
    }

    @Test
    @DirtiesContext
    public void do_not_merge_into_remapped_multimap_variants() throws Exception {
        // given
        Long rs1 = 3000000000L;
        Long rs2 = 3100000000L;
        Long ssToRemap = 5000000000L;
        Long ss2 = 5100000000L;
        Long ss3 = 5200000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        mongoTemplate.insert(createClusteredVariantEntity(asm1, 100L, rs1, 3), getClusteredTable(rs1));
        mongoTemplate.insert(createClusteredVariantEntity(asm1, 200L, rs1, null), getClusteredTable(rs1));

        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 100L, rs1, ssToRemap, NOT_REMAPPED),
                             getSubmittedCollection(ssToRemap));
        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 200L, rs1, ss3, NOT_REMAPPED), getSubmittedCollection(ssToRemap));


        // NOTE: rs2 won't be merged into rs1 because rs1 is multimap
        mongoTemplate.insert(createClusteredVariantEntity(asm2, 100L, rs2, null), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 100L, rs2, ss2, NOT_REMAPPED), getSubmittedCollection(ss2));

        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, 100L, rs1, ssToRemap, asm1);
        sve1Remapped.setMapWeight(3);
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        assertAssembliesPresent(Sets.newTreeSet(asm1, asm2));
    }

    @Test
    @DirtiesContext
    public void do_not_merge_remapped_multimap_variants() throws Exception {
        // given
        Long rs1 = 3100000000L;
        Long rs2 = 3000000000L;
        Long ssToRemap = 5500000000L;
        Long ss2 = 5100000000L;
        Long ss3 = 5200000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        // NOTE: rs1 won't be merged into rs2 because rs1 is multimap
        mongoTemplate.insert(createClusteredVariantEntity(asm1, 100L, rs1, 3), getClusteredTable(rs1));
        mongoTemplate.insert(createClusteredVariantEntity(asm1, 200L, rs1, 3), getClusteredTable(rs1));

        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 100L, rs1, ssToRemap, NOT_REMAPPED),
                             getSubmittedCollection(ssToRemap));
        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 200L, rs1, ss3, NOT_REMAPPED), getSubmittedCollection(ssToRemap));


        mongoTemplate.insert(createClusteredVariantEntity(asm2, 100L, rs2, 3), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 100L, rs2, ss2, NOT_REMAPPED), getSubmittedCollection(ss2));

        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, 100L, rs1, ssToRemap, asm1);
        sve1Remapped.setMapWeight(3);
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        assertAssembliesPresent(Sets.newTreeSet(asm1, asm2));
    }

    @Test
    @DirtiesContext
    public void do_not_merge_multimap_variants_into_remapped_variants() throws Exception {
        // given
        Long rs1 = 3000000000L;
        Long rs2 = 3100000000L;
        Long ssToRemap = 5000000000L;
        Long ss2 = 5100000000L;
        Long ss3 = 5200000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        mongoTemplate.insert(createClusteredVariantEntity(asm1, 100L, rs1, null), getClusteredTable(rs1));

        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 100L, rs1, ssToRemap, NOT_REMAPPED),
                             getSubmittedCollection(ssToRemap));


        // NOTE: rs2 won't be merged into rs1 because rs2 is multimap
        mongoTemplate.insert(createClusteredVariantEntity(asm2, 200L, rs2, 3), getClusteredTable(rs2));
        mongoTemplate.insert(createClusteredVariantEntity(asm2, 300L, rs2, null), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 200L, rs2, ss2, NOT_REMAPPED, 3),
                             getSubmittedCollection(ss2));
        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 300L, rs2, ss3, NOT_REMAPPED),
                             getSubmittedCollection(ss2));

        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, 200L, rs1, ssToRemap, asm1);
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        assertAssembliesPresent(Sets.newTreeSet(asm1, asm2));
    }

    @Test
    @DirtiesContext
    public void do_not_merge_remapped_variant_into_multimap_variants() throws Exception {
        // given
        Long rs1 = 3100000000L;
        Long rs2 = 3000000000L;
        Long ssToRemap = 5500000000L;
        Long ss2 = 5100000000L;
        Long ss3 = 5200000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        // NOTE rs1 won't be merged into rs2 because rs2 is multimap
        mongoTemplate.insert(createClusteredVariantEntity(asm1, 100L, rs1, null), getClusteredTable(rs1));

        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 100L, rs1, ssToRemap, NOT_REMAPPED), getSubmittedCollection(ssToRemap));


        mongoTemplate.insert(createClusteredVariantEntity(asm2, 200L, rs2, 3), getClusteredTable(rs2));
        mongoTemplate.insert(createClusteredVariantEntity(asm2, 300L, rs2, 3), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 200L, rs2, ss2, NOT_REMAPPED), getSubmittedCollection(ss2));
        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 300L, rs2, ss3, NOT_REMAPPED), getSubmittedCollection(ss2));

        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, 200L, rs1, ssToRemap, asm1);
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        assertAssembliesPresent(Sets.newTreeSet(asm1, asm2));
    }

    @Test
    @DirtiesContext
    public void merge_into_remapped_multimap_variants_if_single_mapping_per_assembly()
            throws Exception {
        // given
        Long rs1 = 3000000000L;
        Long rs2 = 3100000000L;
        Long ssToRemap = 5000000000L;
        Long ss2 = 5100000000L;
        Long ss3 = 5200000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";
        String asm3 = "asm3";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        // NOTE: merging into rs1 here is allowed because rs1 maps once in asm1 and once in asm3
        mongoTemplate.insert(createClusteredVariantEntity(asm1, 100L, rs1, null), getClusteredTable(rs1));
        mongoTemplate.insert(createClusteredVariantEntity(asm3, 300L, rs1, null), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 100L, rs1, ssToRemap, NOT_REMAPPED), getSubmittedCollection(ssToRemap));
        mongoTemplate.insert(createSubmittedVariantEntity(asm3, 300L, rs1, ss3, NOT_REMAPPED), getSubmittedCollection(ss3));


        mongoTemplate.insert(createClusteredVariantEntity(asm2, 200L, rs2, null), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 200L, rs2, ss2, NOT_REMAPPED), getSubmittedCollection(ss2));

        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, 200L, rs1, ssToRemap, asm1);
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(0, 3, 0, 1,
                             0, 3, 0, 1);

        assertAssembliesPresent(Sets.newTreeSet(asm1, asm2, asm3));
        assertMergedInto(rs1, rs2, ss2);
    }

    @Test
    @DirtiesContext
    public void merge_multimap_variants_into_remapped_variants_if_single_mapping_per_assembly()
            throws Exception {
        // given
        Long rs1 = 3000000000L;
        Long rs2 = 3100000000L;
        Long ssToRemap = 5000000000L;
        Long ss2 = 5100000000L;
        Long ss3 = 5200000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";
        String asm3 = "asm3";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        mongoTemplate.insert(createClusteredVariantEntity(asm1, 100L, rs1, null), getClusteredTable(rs1));

        mongoTemplate.insert(createSubmittedVariantEntity(asm1, 100L, rs1, ssToRemap, NOT_REMAPPED), getSubmittedCollection(ssToRemap));


        // NOTE: merging into rs1 here is allowed because rs2 maps once in asm2 and once in asm3
        mongoTemplate.insert(createClusteredVariantEntity(asm2, 200L, rs2, null), getClusteredTable(rs2));
        mongoTemplate.insert(createClusteredVariantEntity(asm3, 300L, rs2, null), getClusteredTable(rs2));

        mongoTemplate.insert(createSubmittedVariantEntity(asm2, 200L, rs2, ss2, NOT_REMAPPED), getSubmittedCollection(ss2));
        mongoTemplate.insert(createSubmittedVariantEntity(asm3, 300L, rs2, ss3, NOT_REMAPPED), getSubmittedCollection(ss3));

        assertDatabaseCounts(0, 3, 0, 0,
                             0, 3, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, 200L, rs1, ssToRemap, asm1);
        this.clusterVariants(Collections.singletonList(sve1Remapped));

        // then
        // RS2 to RS1 merge will be confined to the ASM2 assembly. Therefore number of CV and SV operations is just 1.
        assertDatabaseCounts(0, 3, 0, 1,
                             0, 3, 0, 1);

        assertAssembliesPresent(Sets.newTreeSet(asm1, asm2, asm3));
        assertMergedInto(rs1, rs2, ss2);
    }

    private void assertAssembliesPresent(TreeSet<String> expectedAssemblies) {
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class);
        submittedVariants.addAll(mongoTemplate.findAll(DbsnpSubmittedVariantEntity.class));
        assertReferenceSequenceAccessionEqual(expectedAssemblies, submittedVariants);

        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        clusteredVariants.addAll(mongoTemplate.findAll(DbsnpClusteredVariantEntity.class));
        assertAssemblyAccessionEqual(expectedAssemblies, clusteredVariants);
    }

    private ClusteredVariantEntity createClusteredVariantEntity(String assembly, Long start, Long rs,
                                                                Integer mapWeight) {
        ClusteredVariant cv = new ClusteredVariant(assembly, 1000, "1", start, VariantType.SNV, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new ClusteredVariantEntity(rs, cvHash, assembly, 1000, "1", start, VariantType.SNV, false, null, 1,
                                          mapWeight);
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long start, Long rs, Long ss,
                                                                String remappedFrom, Integer mapWeight,
                                                                String reference, String alternate) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, 1000, "project", "1", start, reference, alternate, rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(ss, hash1, submittedClustered, 1,
                                                                                   remappedFrom, LocalDateTime.now(),
                                                                                   null);
        submittedVariantEntity.setMapWeight(mapWeight);
        return submittedVariantEntity;
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long start, Long rs, Long ss,
                                                                String remappedFrom, Integer mapWeight) {
        return createSubmittedVariantEntity(assembly, start, rs, ss, remappedFrom, mapWeight,
                                            defaultReferenceAlleleForTesting, defaultAlternateAlleleForTesting);
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long start, Long rs, Long ss,
                                                                String remappedFrom) {
        return createSubmittedVariantEntity(assembly, start, rs, ss, remappedFrom, null,
                                            defaultReferenceAlleleForTesting, defaultAlternateAlleleForTesting);
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long start, Long rs, Long ss,
                                                                String remappedFrom, String reference,
                                                                String alternate) {
        return createSubmittedVariantEntity(assembly, start, rs, ss, remappedFrom, null,
                                            reference, alternate);
    }

    private void clusterVariants(List<SubmittedVariantEntity> submittedVariantEntities)
            throws Exception {
        clusteringWriterPreMergeAndSplit.write(submittedVariantEntities);
        List<SubmittedVariantOperationEntity> mergeCandidates = new ArrayList<>();
        List<SubmittedVariantOperationEntity> splitCandidates = new ArrayList<>();
        SubmittedVariantOperationEntity tempSVO;
        rsMergeCandidatesReader.open(new ExecutionContext());
        while((tempSVO = rsMergeCandidatesReader.read()) != null) {
            mergeCandidates.add(tempSVO);
        }
        rsSplitCandidatesReader.open(new ExecutionContext());
        while((tempSVO = rsSplitCandidatesReader.read()) != null) {
            splitCandidates.add(tempSVO);
        }
        rsMergeWriter.write(mergeCandidates);
        rsSplitWriter.write(splitCandidates);

        ClusteringMongoReader unclusteredVariantsReader = new ClusteringMongoReader(this.mongoTemplate, ASM_2, 100,
                                                                                    false);
        unclusteredVariantsReader.initializeReader();
        List<SubmittedVariantEntity> unclusteredVariants = new ArrayList<>();
        SubmittedVariantEntity tempSV;
        while((tempSV = unclusteredVariantsReader.read()) != null) {
            unclusteredVariants.add(tempSV);
        }
        unclusteredVariantsReader.close();
        // Cluster non-clustered variants
        clusteringWriterPostMergeAndSplit.write(unclusteredVariants);
        // Spring has a mandatory requirement of even small functionality being writers.
        // To satisfy that, we pass in a dummy object to invoke the writer
        // which basically clears the merge and split operations after they were processed above
        clearRSMergeAndSplitCandidates.write(Collections.singletonList(new Object()));
    }
}
