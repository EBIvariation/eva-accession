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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
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
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * This class test some scenarios of ClusteringWriter for remapped Variants
 * <p>
 * Other test classes in this folder take care of other scenarios.
 */
@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class RemappedVariantsClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    private static final long EVA_CLUSTERED_VARIANT_RANGE_START = 3000000000L;

    private static final long EVA_SUBMITTED_VARIANT_RANGE_START = 5000000000L;

    private static final int TAXONOMY = 100;

    private static final String CONTIG = "chr1";

    private static final String ASM_1 = "asm1";

    private static final String ASM_2 = "asm2";

    private static final String PROJECT_1 = "project_1";

    private static final String PROJECT_2 = "project_2";

    private static final String NOT_REMAPPED = null;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ClusteringCounts clusteringCounts;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    private ClusteringWriter clusteringWriter;

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
        mongoTemplate.getDb().drop();
        clusteringWriter = new ClusteringWriter(mongoTemplate, clusteredVariantAccessioningService,
                                                EVA_SUBMITTED_VARIANT_RANGE_START, EVA_CLUSTERED_VARIANT_RANGE_START,
                                                clusteringCounts, true);
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
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
        clusteringWriter.write(submittedVariantEntityList);

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
        clusteringWriter.write(submittedVariantEntityList);

        //get all submitted variants with assembly asm2 and assert rs id
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class).stream()
                                                                      .filter(s -> s.getReferenceSequenceAccession()
                                                                                    .equals(ASM_2))
                                                                      .collect(Collectors.toList());
        assertEquals(2, submittedVariants.size());
        assertEquals(3000000000L, submittedVariants.get(0).getClusteredVariantAccession().longValue());
        assertEquals(3000000000L, submittedVariants.get(1).getClusteredVariantAccession().longValue());

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(2, clusteredVariants.size());
        assertEquals(3000000000L, clusteredVariants.get(0).getAccession().longValue());
        assertEquals(3000000000L, clusteredVariants.get(1).getAccession().longValue());
        assertEquals(ASM_1, clusteredVariants.stream().filter(s -> s.getStart() == 1000).collect(Collectors.toList())
                                             .get(0).getAssemblyAccession());
        assertEquals(ASM_2, clusteredVariants.stream().filter(s -> s.getStart() == 2000).collect(Collectors.toList())
                                             .get(0).getAssemblyAccession());

        //assert submitted Variant Operation Entity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        assertEquals(1, submittedVariantOperationEntities.size());
        SubmittedVariantOperationEntity submittedVariantOperationEntity = submittedVariantOperationEntities.get(0);
        assertEquals(5000000006L, submittedVariantOperationEntity.getAccession().longValue());
        assertEquals(EventType.UPDATED, submittedVariantOperationEntity.getEventType());
        assertEquals("Original rs3000000006 was merged into rs3000000000.",
                     submittedVariantOperationEntity.getReason());

        //assert Clustered Variant Operation Entity
        List<ClusteredVariantOperationEntity> operationEntities = mongoTemplate.findAll(
                ClusteredVariantOperationEntity.class);
        assertEquals(1, operationEntities.size());
        ClusteredVariantOperationEntity clusteredVariantOperationEntity = operationEntities.get(0);
        assertEquals(3000000006L, clusteredVariantOperationEntity.getAccession().longValue());
        assertEquals(EventType.MERGED, clusteredVariantOperationEntity.getEventType());
        assertEquals(3000000000L, clusteredVariantOperationEntity.getMergedInto().longValue());
        assertEquals("Original rs3000000006 was merged into rs3000000000.",
                     clusteredVariantOperationEntity.getReason());
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
        clusteringWriter.write(submittedVariantEntityList);

        //get all submitted variants for assembly asm2 and assert rs id
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class).stream()
                                                                      .filter(s -> s.getReferenceSequenceAccession()
                                                                                    .equals(ASM_2))
                                                                      .collect(Collectors.toList());
        assertEquals(1, submittedVariants.size());
        assertEquals(3000000000L, submittedVariants.get(0).getClusteredVariantAccession().longValue());

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(1, clusteredVariants.size());
        assertEquals(3000000000L, clusteredVariants.get(0).getAccession().longValue());

        //assert submittedVariationOperationEntity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        SubmittedVariantOperationEntity submittedVariantOperationEntity = submittedVariantOperationEntities.get(0);
        assertEquals(1, submittedVariantOperationEntities.size());
        assertEquals(5000000000L, submittedVariantOperationEntity.getAccession().longValue());
        assertEquals(EventType.UPDATED, submittedVariantOperationEntity.getEventType());
        assertEquals("Clustering submitted variant 5000000000 with rs3000000000",
                     submittedVariantOperationEntity.getReason());
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
        clusteringWriter.write(submittedVariantEntityList);

        //get all submitted variants with assembly asm2 and check rs id
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class).stream()
                                                                      .filter(s -> s.getReferenceSequenceAccession()
                                                                                    .equals(ASM_2))
                                                                      .collect(Collectors.toList());
        assertEquals(2, submittedVariants.size());
        assertEquals(3000000006L, submittedVariants.get(0).getClusteredVariantAccession().longValue());
        assertEquals(3000000006L, submittedVariants.get(1).getClusteredVariantAccession().longValue());

        //get all clusteredVariantEntity and check rs id for all
        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(1, clusteredVariants.size());
        assertEquals(3000000006L, clusteredVariants.get(0).getAccession().longValue());

        //assert submittedVariationOperationEntity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        SubmittedVariantOperationEntity submittedVariantOperationEntity = submittedVariantOperationEntities.get(0);
        assertEquals(1, submittedVariantOperationEntities.size());
        assertEquals(5000000000L, submittedVariantOperationEntity.getAccession().longValue());
        assertEquals(EventType.UPDATED, submittedVariantOperationEntity.getEventType());
        assertEquals("Clustering submitted variant 5000000000 with rs3000000006",
                     submittedVariantOperationEntity.getReason());
    }

    @Test
    @DirtiesContext
    public void test() throws Exception {
        //original submitted variants
        SubmittedVariantEntity submittedVariantEntity1 = getSubmittedVariantEntity(ASM_1, PROJECT_1, 1000, 3000000006L,
                                                                                   5000000001L, "", "TT", NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity1, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity2 = getSubmittedVariantEntity(ASM_1, PROJECT_1, 1000, 3000000006L,
                                                                                   5000000000L, "A", "AT",
                                                                                   NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity2, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity3 = getSubmittedVariantEntity(ASM_1, PROJECT_2, 3000, 3000000007L,
                                                                                   5000000002L, "C", "G", NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity3, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity4 = getSubmittedVariantEntity(ASM_1, PROJECT_2, 4000, 3000000008L,
                                                                                   5000000003L, "G", "C", NOT_REMAPPED);
        mongoTemplate.insert(submittedVariantEntity4, SUBMITTED_VARIANT_COLLECTION);

        //clustered variants
        ClusteredVariantEntity clusteredVariantEntity1 = getClusteredVariantEntity(ASM_1, 1000, 3000000006L,
                                                                                   VariantType.INS);
        mongoTemplate.insert(clusteredVariantEntity1, CLUSTERED_VARIANT_COLLECTION);
        ClusteredVariantEntity clusteredVariantEntity2 = getClusteredVariantEntity(ASM_2, 3000, 3000000005L,
                                                                                   VariantType.SNV);
        mongoTemplate.insert(clusteredVariantEntity2, CLUSTERED_VARIANT_COLLECTION);
        ClusteredVariantEntity clusteredVariantEntity3 = getClusteredVariantEntity(ASM_1, 4000, 3000000008L,
                                                                                   VariantType.INS);
        mongoTemplate.insert(clusteredVariantEntity3, CLUSTERED_VARIANT_COLLECTION);

        //submitted variants after remapping
        SubmittedVariantEntity submittedVariantEntity5 = getSubmittedVariantEntity(ASM_2, PROJECT_1, 1500, 3000000006L,
                                                                                   5000000005L, "", "TT", ASM_1);
        mongoTemplate.insert(submittedVariantEntity5, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity6 = getSubmittedVariantEntity(ASM_2, PROJECT_1, 1000, 3000000006L,
                                                                                   5000000004L, "", "T", ASM_1);
        mongoTemplate.insert(submittedVariantEntity6, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity7 = getSubmittedVariantEntity(ASM_2, PROJECT_2, 3000, 3000000007L,
                                                                                   5000000006L, "C", "G", ASM_1);
        mongoTemplate.insert(submittedVariantEntity7, SUBMITTED_VARIANT_COLLECTION);
        SubmittedVariantEntity submittedVariantEntity8 = getSubmittedVariantEntity(ASM_2, PROJECT_2, 4000, 3000000008L,
                                                                                   5000000007L, "G", "CC", ASM_1);
        mongoTemplate.insert(submittedVariantEntity8, SUBMITTED_VARIANT_COLLECTION);

        //asm2 clustered
        List<SubmittedVariantEntity> submittedVariantEntityList = new ArrayList<>();
        submittedVariantEntityList.add(submittedVariantEntity5);
        submittedVariantEntityList.add(submittedVariantEntity7);
        clusteringWriter.write(submittedVariantEntityList);
        submittedVariantEntityList.clear();
        submittedVariantEntityList.add(submittedVariantEntity6);
        submittedVariantEntityList.add(submittedVariantEntity8);
        clusteringWriter.write(submittedVariantEntityList);

        //assert submittedVariantEntity
        List<SubmittedVariantEntity> submittedVariantEntities = mongoTemplate.findAll(SubmittedVariantEntity.class);
        assertEquals(8, submittedVariantEntities.size());

        //assert clusteredVariantEntity
        List<ClusteredVariantEntity> clusteredVariantEntities = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertEquals(6, clusteredVariantEntities.size());
        List<ClusteredVariantEntity> expectedClusteredVariantEntityWithNewRS = clusteredVariantEntities.stream()
                                        .filter(cls -> cls.getAccession().equals(3000000000L))
                                        .collect(Collectors.toList());
        assertEquals(1, expectedClusteredVariantEntityWithNewRS.size());
        assertEquals(Long.valueOf(3000000000L), expectedClusteredVariantEntityWithNewRS.get(0).getAccession());
        assertEquals(1000, expectedClusteredVariantEntityWithNewRS.get(0).getStart());

        //assert submittedVariantOperationEntity
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = mongoTemplate.findAll(
                SubmittedVariantOperationEntity.class);
        assertEquals(3, submittedVariantOperationEntities.size());
        List<SubmittedVariantOperationEntity> expectedSubmittedOperationEntityWithNewRS =
                submittedVariantOperationEntities.stream()
                                                 .filter(sve -> sve.getAccession().equals(5000000004L))
                                                 .collect(Collectors.toList());
        assertEquals(1, expectedSubmittedOperationEntityWithNewRS.size());
        assertEquals(EventType.UPDATED, expectedSubmittedOperationEntityWithNewRS.get(0).getEventType());
        assertEquals("Original rs3000000006 was merged into rs3000000000.",
                     expectedSubmittedOperationEntityWithNewRS.get(0).getReason());

        //assert clusteredVariantOperationEntity
        List<ClusteredVariantOperationEntity> clusteredVariantOperationEntities = mongoTemplate.findAll(
                ClusteredVariantOperationEntity.class);
        assertEquals(1, clusteredVariantOperationEntities.size());
        ClusteredVariantOperationEntity expectedClusteredVariantOperationEntity = clusteredVariantOperationEntities.get(0);
        assertEquals(Long.valueOf(3000000006L), expectedClusteredVariantOperationEntity.getAccession());
        assertEquals(Long.valueOf(3000000000L), expectedClusteredVariantOperationEntity.getMergedInto());
        assertEquals(EventType.MERGED, expectedClusteredVariantOperationEntity.getEventType());
        assertEquals("Due to RS Split new accession id 3000000000 created for remapped accession 3000000006",
                     expectedClusteredVariantOperationEntity.getReason());
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(String assembly, String project, long start, Long rs,
                                                             long ss, String ref, String alt, String remappedFrom) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, TAXONOMY, project, CONTIG, start, ref, alt,
                                                                   rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        return new SubmittedVariantEntity(ss, hash1, submittedClustered, 1, remappedFrom, LocalDateTime.now(), null);
    }

    private SubmittedVariantEntity getSubmittedVariantEntity(String assembly, String project, long start, Long rs,
                                                             long ss, String remappedFrom) {
        return getSubmittedVariantEntity(assembly, project, start, rs, ss, "T", "A", remappedFrom);
    }

    private ClusteredVariantEntity getClusteredVariantEntity(String assembly, long start, Long rs) {
        return getClusteredVariantEntity(assembly, start, rs, VariantType.SNV);
    }

    private ClusteredVariantEntity getClusteredVariantEntity(String assembly, long start, Long rs, VariantType type) {
        ClusteredVariant cv = new ClusteredVariant(assembly, TAXONOMY, CONTIG, start, type, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new ClusteredVariantEntity(rs, cvHash, assembly, TAXONOMY, CONTIG, start, type, false, null, 1, null);
    }
}
