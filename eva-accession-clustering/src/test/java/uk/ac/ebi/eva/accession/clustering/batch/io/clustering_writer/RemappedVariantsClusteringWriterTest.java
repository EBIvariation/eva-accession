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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReader;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.RSSplitWriter;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.mongodb.readers.MongoDbCursorItemReader;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATED_RS_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;

/**
 * This class test some scenarios of ClusteringWriter for remapped Variants
 * <p>
 * Other test classes in this folder take care of other scenarios.
 */
@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-writer-test.properties")
public class RemappedVariantsClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    private static final int TAXONOMY = 100;

    private static final String CONTIG = "chr1";

    private static String ASM_1;

    private static String ASM_2;

    private static final String PROJECT_1 = "project_1";

    private static final String PROJECT_2 = "project_2";

    private static final String NOT_REMAPPED = null;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private InputParameters inputParameters;

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
    private RSSplitWriter rsSplitWriter;

    @Autowired
    @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES)
    private ItemWriter clearRSMergeAndSplitCandidates;

    @Autowired
    @Qualifier(BACK_PROPAGATED_RS_WRITER)
    private ItemWriter<SubmittedVariantEntity> backPropagatedRSWriter;

    @MockBean
    private JobExecution jobExecution;

    private Function<ISubmittedVariant, String> hashingFunction;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        ASM_1 = inputParameters.getRemappedFrom();
        ASM_2 = inputParameters.getAssemblyAccession();
        mongoTemplate.getDb().drop();
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

        Mockito.when(jobExecution.getJobId()).thenReturn(1L);
        rsSplitWriter.setJobExecution(jobExecution);
        clusteringWriterPreMergeAndSplit.setJobExecution(jobExecution);
        clusteringWriterPostMergeAndSplit.setJobExecution(jobExecution);
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    public void test_variant_clustered_rs_id_does_not_exist() throws Exception {
        SubmittedVariantEntity submittedVariantEntity1 = getSubmittedVariantEntity(ASM_1, PROJECT_1, 1000, 3000000001L,
                                                                                   5000000000L, NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity1, SUBMITTED_VARIANT_COLLECTION);

        ClusteredVariantEntity clusteredVariantEntity1 = getClusteredVariantEntity(ASM_1, 1000, 3000000001L);
        mongoTemplate.insert(clusteredVariantEntity1, CLUSTERED_VARIANT_COLLECTION);

        //variant remapped from asm1 to asm2
        SubmittedVariantEntity submittedVariantEntity2 = getSubmittedVariantEntity(ASM_2, PROJECT_1, 2000, 3000000001L,
                                                                                   5000000000L, ASM_1);
        mongoTemplate.insert(submittedVariantEntity2, SUBMITTED_VARIANT_COLLECTION);

        //asm2 clustered
        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        submittedVariantEntityList.add(submittedVariantEntity2);
        this.clusterVariants(submittedVariantEntityList);

        //get all submitted variants with assembly asm2 and assert rs id
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class).stream()
                                                                      .filter(s -> s.getReferenceSequenceAccession()
                                                                                    .equals(ASM_2))
                                                                      .collect(Collectors.toList());
        assertEquals(1, submittedVariants.size());
        assertEquals(3000000001L, submittedVariants.get(0).getClusteredVariantAccession().longValue());

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(2, clusteredVariants.size());
        assertEquals(3000000001L, clusteredVariants.get(0).getAccession().longValue());
        assertEquals(3000000001L, clusteredVariants.get(1).getAccession().longValue());
        assertEquals(ASM_1, clusteredVariants.stream().filter(s -> s.getStart() == 1000).collect(Collectors.toList())
                                             .get(0).getAssemblyAccession());
        assertEquals(ASM_2, clusteredVariants.stream().filter(s -> s.getStart() == 2000).collect(Collectors.toList())
                                             .get(0).getAssemblyAccession());

        //assert no submitted variant operations should happen
        List<SubmittedVariantOperationEntity> submittedVariantOperations = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        assertEquals(0, submittedVariantOperations.size());

        //assert no clustered variant operation should happen
        List<ClusteredVariantOperationEntity> clusteredVariantOperations = mongoTemplate.findAll(
                ClusteredVariantOperationEntity.class);
        assertEquals(0, clusteredVariantOperations.size());
    }

    @Test
    @DirtiesContext
    public void test_variant_clustered_rs_id_exist() throws Exception {
        SubmittedVariantEntity submittedVariantEntity1 = getSubmittedVariantEntity(ASM_1, PROJECT_1, 1000, 3000000000L,
                                                                                   5000000000L, NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity1, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity2 = getSubmittedVariantEntity(ASM_2, PROJECT_2, 2000, 3000000006L,
                                                                                   5000000006L, NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity2, SUBMITTED_VARIANT_COLLECTION);

        ClusteredVariantEntity clusteredVariantEntity1 = getClusteredVariantEntity(ASM_1, 1000, 3000000000L);
        mongoTemplate.insert(clusteredVariantEntity1, CLUSTERED_VARIANT_COLLECTION);
        ClusteredVariantEntity clusteredVariantEntity2 = getClusteredVariantEntity(ASM_2, 2000, 3000000006L);
        mongoTemplate.insert(clusteredVariantEntity2, CLUSTERED_VARIANT_COLLECTION);

        //variant remapped from asm1 to asm 2
        SubmittedVariantEntity submittedVariantEntity3 = getSubmittedVariantEntity(ASM_2, PROJECT_1, 2000, 3000000000L,
                                                                                   5000000000L, ASM_1);
        mongoTemplate.insert(submittedVariantEntity3, SUBMITTED_VARIANT_COLLECTION);

        //asm2 clustered
        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        submittedVariantEntityList.add(submittedVariantEntity2);
        submittedVariantEntityList.add(submittedVariantEntity3);
        this.clusterVariants(submittedVariantEntityList);

        //get all submitted variants with assembly asm2 and assert rs id
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class).stream()
                                                                      .filter(s -> s.getReferenceSequenceAccession()
                                                                                    .equals(ASM_2))
                                                                      .collect(Collectors.toList());
        assertEquals(2, submittedVariants.size());

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(2, clusteredVariants.size());
        List<ClusteredVariantEntity> clusteredVariantsASM_1 = clusteredVariants.stream()
                .filter(cve->cve.getAssemblyAccession().equals(ASM_1)).collect(Collectors.toList());
        List<ClusteredVariantEntity> clusteredVariantsASM_2 = clusteredVariants.stream()
                .filter(cve->cve.getAssemblyAccession().equals(ASM_2)).collect(Collectors.toList());
        assertEquals(1, clusteredVariantsASM_1.size());
        assertEquals(1, clusteredVariantsASM_2.size());
        assertEquals(3000000000L, clusteredVariantsASM_1.get(0).getAccession().longValue());
        assertEquals(3000000000L, clusteredVariantsASM_2.get(0).getAccession().longValue());
        assertEquals(1000, clusteredVariantsASM_1.get(0).getStart());
        assertEquals(2000, clusteredVariantsASM_2.get(0).getStart());

        //assert submitted Variant Operation Entity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        assertEquals(1, submittedVariantOperationEntities.size());
        SubmittedVariantOperationEntity submittedVariantOperationEntity = submittedVariantOperationEntities.get(0);
        assertEquals(5000000006L, submittedVariantOperationEntity.getAccession().longValue());
        assertEquals(3000000006L,
                     submittedVariantOperationEntity.getInactiveObjects().get(0).getClusteredVariantAccession()
                                                    .longValue());
        assertEquals(EventType.UPDATED, submittedVariantOperationEntity.getEventType());
    }

    @Test
    @DirtiesContext
    public void test_variant_not_clustered_rs_id_does_not_exist() throws Exception {
        SubmittedVariantEntity submittedVariantEntity1 = getSubmittedVariantEntity(ASM_1, PROJECT_1, 1000, null,
                                                                                   5000000000L, NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity1, SUBMITTED_VARIANT_COLLECTION);

        //variant remapped from asm1 to asm2
        SubmittedVariantEntity submittedVariantEntity2 = getSubmittedVariantEntity(ASM_2, PROJECT_1, 2000, null,
                                                                                   5000000000L, ASM_1);
        mongoTemplate.insert(submittedVariantEntity2, SUBMITTED_VARIANT_COLLECTION);

        //asm2 clustered
        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        submittedVariantEntityList.add(submittedVariantEntity2);

        this.clusterVariants(submittedVariantEntityList);


        //get all submitted variants for assembly asm2 and assert rs id
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class);
        assertEquals(2, submittedVariants.size());
        assertEquals(new HashSet<>(Collections.singletonList(3000000000L)),
                submittedVariants.stream().map(SubmittedVariantEntity::getClusteredVariantAccession)
                        .filter(Objects::nonNull).collect(Collectors.toSet())
                );
        assertEquals(new HashSet<>(Collections.singletonList(3000000000L)),
                submittedVariants.stream().map(SubmittedVariantEntity::getBackPropagatedVariantAccession)
                        .filter(Objects::nonNull).collect(Collectors.toSet())
        );
        assertTrue(submittedVariants.stream().map(SubmittedVariantEntity::getReferenceSequenceAccession).collect(
                                 Collectors.toList()).containsAll(Arrays.asList(ASM_1, ASM_2)));

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        // One record: Newly generated RS in the new assembly
        // No record created for the RS back-propagated to the older assembly
        assertEquals(1, clusteredVariants.size());
        assertEquals(3000000000L, clusteredVariants.get(0).getAccession().longValue());

        //assert submittedVariationOperationEntity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        // Two events: one for clustering the remapped variant with a new RS ID
        // and another for back-propagating that RS ID to the original variant
        assertEquals(2, submittedVariantOperationEntities.size());
        assertEquals(5000000000L, submittedVariantOperationEntities.get(0).getAccession().longValue());
        assertEquals(5000000000L, submittedVariantOperationEntities.get(1).getAccession().longValue());
        assertTrue(submittedVariantOperationEntities.stream().map(SubmittedVariantOperationEntity::getEventType)
                .collect(Collectors.toList()).containsAll(Arrays.asList(EventType.UPDATED,
                        EventType.RS_BACK_PROPAGATED)));
        assertEquals("Clustering submitted variant 5000000000 with rs3000000000",
                     submittedVariantOperationEntities.stream().filter(svoe -> svoe.getInactiveObjects().get(0)
                                                                                   .getReferenceSequenceAccession()
                                                                                   .equals(ASM_2)).findFirst().get()
                                                      .getReason());
        assertEquals("Back-propagating rs3000000000 for submitted variant ss5000000000 after remapping to asm2.",
                     submittedVariantOperationEntities.stream().filter(svoe -> svoe.getInactiveObjects().get(0)
                                                                                   .getReferenceSequenceAccession()
                                                                                   .equals(ASM_1)).findFirst().get()
                                                      .getReason());
    }

    @Test
    @DirtiesContext
    public void test_variant_not_clustered_rs_id_exist() throws Exception {
        SubmittedVariantEntity submittedVariantEntity1 = getSubmittedVariantEntity(ASM_1, PROJECT_1, 1000, null,
                                                                                   5000000000L, NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity1, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity2 = getSubmittedVariantEntity(ASM_2, PROJECT_2, 2000, 3000000006L,
                                                                                   5000000006L, NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity2, SUBMITTED_VARIANT_COLLECTION);

        ClusteredVariantEntity clusteredVariantEntity2 = getClusteredVariantEntity(ASM_2, 2000, 3000000006L);
        mongoTemplate.insert(clusteredVariantEntity2, CLUSTERED_VARIANT_COLLECTION);

        //variant remapped from asm1 to asm2
        SubmittedVariantEntity submittedVariantEntity3 = getSubmittedVariantEntity(ASM_2, PROJECT_1, 2000, null,
                                                                                   5000000000L, ASM_1);
        mongoTemplate.insert(submittedVariantEntity3, SUBMITTED_VARIANT_COLLECTION);

        //asm2 clustered
        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        submittedVariantEntityList.add(submittedVariantEntity2);
        submittedVariantEntityList.add(submittedVariantEntity3);
        this.clusterVariants(submittedVariantEntityList);

        //get all submitted variants
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class);
        // submittedVariantEntity2 and submittedVariantEntity3 will get the same RS in ASM2
        assertEquals(3, submittedVariants.size());
        // submittedVariantEntity1 will receive the back-propagated RS
        assertEquals(submittedVariants.stream().map(SubmittedVariantEntity::getClusteredVariantAccession)
                        .filter(Objects::nonNull).collect(Collectors.toSet()),
                     new HashSet<>(Collections.singletonList(3000000006L)));
        assertEquals(submittedVariants.stream().map(SubmittedVariantEntity::getBackPropagatedVariantAccession)
                        .filter(Objects::nonNull).collect(Collectors.toSet()),
                new HashSet<>(Collections.singletonList(3000000006L)));
        assertTrue(submittedVariants.stream().map(SubmittedVariantEntity::getReferenceSequenceAccession).collect(
                Collectors.toList()).containsAll(Arrays.asList(ASM_1, ASM_2)));

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        // Two records: Newly generated RS in the new assembly
        // No RS entries are created for back-propagated RS to the older assembly
        assertEquals(1, clusteredVariants.size());
        assertEquals(3000000006L, clusteredVariants.get(0).getAccession().longValue());
        assertTrue(clusteredVariants.stream().map(ClusteredVariantEntity::getAssemblyAccession).collect(
                Collectors.toList()).containsAll(Arrays.asList(ASM_2)));

        //assert submittedVariationOperationEntity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        // Two events: one for clustering the remapped variant with a new RS ID
        // and another for back-propagating that RS ID to the original variant
        assertEquals(2, submittedVariantOperationEntities.size());
        assertEquals(5000000000L, submittedVariantOperationEntities.get(0).getAccession().longValue());
        assertEquals(5000000000L, submittedVariantOperationEntities.get(1).getAccession().longValue());
        assertTrue(submittedVariantOperationEntities.stream().map(SubmittedVariantOperationEntity::getEventType)
                .collect(Collectors.toList()).containsAll(Arrays.asList(EventType.UPDATED,
                        EventType.RS_BACK_PROPAGATED)));
        assertEquals("Clustering submitted variant 5000000000 with rs3000000006",
                     submittedVariantOperationEntities.stream().filter(svoe -> svoe.getInactiveObjects().get(0)
                                                                                   .getReferenceSequenceAccession()
                                                                                   .equals(ASM_2)).findFirst().get()
                                                      .getReason());
        assertEquals("Back-propagating rs3000000006 for submitted variant ss5000000000 after remapping to asm2.",
                     submittedVariantOperationEntities.stream().filter(svoe -> svoe.getInactiveObjects().get(0)
                                                                                   .getReferenceSequenceAccession()
                                                                                   .equals(ASM_1)).findFirst().get()
                                                      .getReason());
    }

    /**
     * This test is for the testing the detection of Merge and RS split candidates.
     * These are already clustered variants, but due to remapping, the variants with same rs id might end up in two
     * different locations or different types. These needs to be identified for rectifying later.
     *
     * --------------------Before Clustering--------------------
     *
     *              SubmittedVariantEntity
     * SS	RS	ASM	    STUDY	CONTIG	POS	    REF	    ALT
     * 500	306	ASM1	PRJEB1	Chr1	1000	A	    T  (original)
     * 501	306	ASM1	PRJEB2	Chr1	1000	A	    T  (original)
     * 500	306	ASM2	PRJEB1	Chr1	1000	A	    T  (remapped)
     * 501	306	ASM2	PRJEB2	Chr1	1500	A	    T   (remapped)
     *
     *              ClusteredVariantEntity
     * RS	HASH                ASM	    POS	    CONTIG	TYPE
     * 306  ASM1_Chr1_1000_SNV	ASM1	1000	Chr1	SNV
     *
     * SS id 500 and 501 has same RS because of remapping, but they are now at different positions and can't have same RS id.
     * These needs to be identified and stored in submittedVariantOperationEntity table for rectification.
     *
     * --------------------After Detection--------------------
     *
     *              SubmittedVariantOperationEntity
     * ACCESSION    EVENT_TYPE          REASON                  INACTIVe_OBJECTS
     * 306          RS_SPLIT_CANDIDATE  Hash mismatch with 306  {ss-500 and ss-501}
     */
    @Test
    @DirtiesContext
    public void test_detect_RS_MERGE_AND_SPLIT_different_batches() throws Exception {
        //Batch 1
        SubmittedVariantEntity submittedVariantEntityRemapped1 = getSubmittedVariantEntity(ASM_2, "project_1",
                5000000001L, 3000000001L, 1000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped1, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntityRemapped2 = getSubmittedVariantEntity(ASM_2, "project_2",
                5000000002L, 3000000003L, 2000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped2, SUBMITTED_VARIANT_COLLECTION);

        clusteringWriterPreMergeAndSplit.write(Arrays.asList(submittedVariantEntityRemapped1, submittedVariantEntityRemapped2));

        //Batch 2 - split event
        SubmittedVariantEntity submittedVariantEntityRemapped3 = getSubmittedVariantEntity(ASM_2, "project_3",
                5000000003L, 3000000001L, 3000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped3, SUBMITTED_VARIANT_COLLECTION);

        clusteringWriterPreMergeAndSplit.write(Arrays.asList(submittedVariantEntityRemapped3));

        //Batch 3 - split event
        SubmittedVariantEntity submittedVariantEntityRemapped4 = getSubmittedVariantEntity(ASM_2, "project_4",
                5000000004L, 3000000001L, 4000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped4, SUBMITTED_VARIANT_COLLECTION);

        clusteringWriterPreMergeAndSplit.write(Arrays.asList(submittedVariantEntityRemapped4));

        //Batch 4 - merge event
        SubmittedVariantEntity submittedVariantEntityRemapped5 = getSubmittedVariantEntity(ASM_2, "project_5",
                5000000005L, 3000000002L, 2000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped5, SUBMITTED_VARIANT_COLLECTION);

        clusteringWriterPreMergeAndSplit.write(Arrays.asList(submittedVariantEntityRemapped5));

        //Batch 5 - merge event
        SubmittedVariantEntity submittedVariantEntityRemapped6 = getSubmittedVariantEntity(ASM_2, "project_6",
                5000000006L, 3000000002L, 2000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped6, SUBMITTED_VARIANT_COLLECTION);

        clusteringWriterPreMergeAndSplit.write(Arrays.asList(submittedVariantEntityRemapped6));

        //Assert
        assertMergeAndSplitCandidatesScenarios(Arrays.asList(submittedVariantEntityRemapped1,
                submittedVariantEntityRemapped2,submittedVariantEntityRemapped3,submittedVariantEntityRemapped4,
                submittedVariantEntityRemapped5,submittedVariantEntityRemapped6));
    }

    @Test
    @DirtiesContext
    public void test_detect_RS_MERGE_AND_SPLIT_same_batch() throws Exception {
        SubmittedVariantEntity submittedVariantEntityRemapped1 = getSubmittedVariantEntity(ASM_2, "project_1",
                5000000001L, 3000000001L, 1000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped1, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntityRemapped2 = getSubmittedVariantEntity(ASM_2, "project_2",
                5000000002L, 3000000003L, 2000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped2, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntityRemapped3 = getSubmittedVariantEntity(ASM_2, "project_3",
                5000000003L, 3000000001L, 3000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped3, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntityRemapped4 = getSubmittedVariantEntity(ASM_2, "project_4",
                5000000004L, 3000000001L, 4000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped4, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntityRemapped5 = getSubmittedVariantEntity(ASM_2, "project_5",
                5000000005L, 3000000002L, 2000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped5, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntityRemapped6 = getSubmittedVariantEntity(ASM_2, "project_6",
                5000000006L, 3000000002L, 2000,"A", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntityRemapped6, SUBMITTED_VARIANT_COLLECTION);

        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        submittedVariantEntityList.add(submittedVariantEntityRemapped1);
        submittedVariantEntityList.add(submittedVariantEntityRemapped2);
        submittedVariantEntityList.add(submittedVariantEntityRemapped3);
        submittedVariantEntityList.add(submittedVariantEntityRemapped4);
        submittedVariantEntityList.add(submittedVariantEntityRemapped5);
        submittedVariantEntityList.add(submittedVariantEntityRemapped6);
        clusteringWriterPreMergeAndSplit.write(Arrays.asList(submittedVariantEntityRemapped1, submittedVariantEntityRemapped2,
                                                             submittedVariantEntityRemapped3, submittedVariantEntityRemapped4,
                                                             submittedVariantEntityRemapped5, submittedVariantEntityRemapped6));

        //Assert
        assertMergeAndSplitCandidatesScenarios(Arrays.asList(submittedVariantEntityRemapped1,
                submittedVariantEntityRemapped2,submittedVariantEntityRemapped3,submittedVariantEntityRemapped4,
                submittedVariantEntityRemapped5,submittedVariantEntityRemapped6));
    }

    private void assertMergeAndSplitCandidatesScenarios(List<SubmittedVariantEntity> submittedVariantEntities){
        //assert clusteredVariantEntity
        List<ClusteredVariantEntity> clusteredVariantEntities = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(4, clusteredVariantEntities.size());
        assertEquals(1,clusteredVariantEntities.stream().filter(cve->cve.getAccession().equals(3000000003L)).count());
        assertEquals(3,clusteredVariantEntities.stream().filter(cve->cve.getAccession().equals(3000000001L)).count());

        //assert merge event
        SubmittedVariantOperationEntity submittedVariantOperationEntityMerge =
                mongoTemplate.findAll(SubmittedVariantOperationEntity.class).stream()
                        .filter(svoe->svoe.getEventType().equals(EventType.RS_MERGE_CANDIDATES))
                        .collect(Collectors.toList()).get(0);
        assertEquals(EventType.RS_MERGE_CANDIDATES, submittedVariantOperationEntityMerge.getEventType());
        assertEquals(3000000003L,submittedVariantOperationEntityMerge.getAccession().longValue());
        assertEquals("RS mismatch with 3000000003", submittedVariantOperationEntityMerge.getReason());
        assertEquals(3, submittedVariantOperationEntityMerge.getInactiveObjects().size());
        assertTrue(submittedVariantOperationEntityMerge.getInactiveObjects().containsAll(
                Arrays.asList(new SubmittedVariantInactiveEntity(submittedVariantEntities.get(1)),
                        new SubmittedVariantInactiveEntity(submittedVariantEntities.get(4)),
                new SubmittedVariantInactiveEntity(submittedVariantEntities.get(5)))));

        //assert split event
        SubmittedVariantOperationEntity submittedVariantOperationEntitySplit =
                mongoTemplate.findAll(SubmittedVariantOperationEntity.class).stream()
                        .filter(svoe->svoe.getEventType().equals(EventType.RS_SPLIT_CANDIDATES))
                        .collect(Collectors.toList()).get(0);
        assertEquals(EventType.RS_SPLIT_CANDIDATES, submittedVariantOperationEntitySplit.getEventType());
        assertEquals(3000000001L,submittedVariantOperationEntitySplit.getAccession().longValue());
        assertEquals("Hash mismatch with 3000000001", submittedVariantOperationEntitySplit.getReason());
        assertEquals(3, submittedVariantOperationEntitySplit.getInactiveObjects().size());
        assertTrue(submittedVariantOperationEntitySplit.getInactiveObjects().containsAll(
                Arrays.asList(new SubmittedVariantInactiveEntity(submittedVariantEntities.get(0)),
                        new SubmittedVariantInactiveEntity(submittedVariantEntities.get(2)),
                        new SubmittedVariantInactiveEntity(submittedVariantEntities.get(3)))));
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(String assembly, String project, long ss, Long rs, long start,
                                                             String ref, String alt, String remappedFrom) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, TAXONOMY, project, CONTIG, start, ref, alt,
                                                                   rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        return new SubmittedVariantEntity(ss, hash1, submittedClustered, 1, remappedFrom, LocalDateTime.now(), null);
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(String assembly, String project, long start, Long rs,
                                                             long ss, String remappedFrom) {
        return getSubmittedVariantEntity(assembly, project, ss, rs, start, "T", "A", remappedFrom);
    }

    private ClusteredVariantEntity getClusteredVariantEntity(String assembly, long start, Long rs) {
        return getClusteredVariantEntity(assembly, start, rs, VariantType.SNV);
    }

    private ClusteredVariantEntity getClusteredVariantEntity(String assembly, long start, Long rs, VariantType type) {
        ClusteredVariant cv = new ClusteredVariant(assembly, TAXONOMY, CONTIG, start, type, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new ClusteredVariantEntity(rs, cvHash, assembly, TAXONOMY, CONTIG, start, type, false, null, 1, null);
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
        ClusteringMongoReader originalAssemblyUnclusteredVariantsReader =
                new ClusteringMongoReader(this.mongoTemplate, ASM_1, 100, false);
        originalAssemblyUnclusteredVariantsReader.initializeReader();
        while((tempSV = originalAssemblyUnclusteredVariantsReader.read()) != null) {
            backPropagatedRSWriter.write(Collections.singletonList(tempSV));
        }
    }
}
