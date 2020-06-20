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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.assertj.core.util.Sets;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;

import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertAccessionEqual;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertAssemblyAccessionEqual;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertClusteredVariantAccessionEqual;
import static uk.ac.ebi.eva.accession.clustering.test.VariantAssertions.assertReferenceSequenceAccessionEqual;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class ClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    private static final String PROJECT_ACCESSION = "projectId_1";

    public static final String CLUSTERED_VARIANT_ENTITY = "clusteredVariantEntity";

    public static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    public static final long EVA_CLUSTERED_VARIANT_RANGE_START = 3000000000L;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    @Autowired
    private ContiguousIdBlockRepository contiguousIdBlockRepository;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    private ClusteringWriter clusteringWriter;

    private Function<ISubmittedVariant, String> hashingFunction;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        clusteringWriter = new ClusteringWriter(mongoTemplate,
                                                clusteredVariantAccessioningService);
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
        mongoTemplate.dropCollection(ClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(SubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(ClusteredVariantOperationEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantOperationEntity.class);
    }

    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    @DirtiesContext
    public void writer() throws Exception {
        List<SubmittedVariantEntity> submittedVariantEntities = createSubmittedVariantEntities();
        clusteringWriter.write(submittedVariantEntities);
        assertClusteredVariantsCreated();
        assertSubmittedVariantsUpdated();
    }

    private List<SubmittedVariantEntity> createSubmittedVariantEntities() {
        List<SubmittedVariantEntity> submittedVariantEntities = new ArrayList<>();
        SubmittedVariant submittedVariant1 = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                    PROJECT_ACCESSION, "1", 1000L, "T", "A");
        SubmittedVariantEntity submittedVariantEntity1 = createSubmittedVariantEntity(1L, submittedVariant1);
        //Different alleles
        SubmittedVariant submittedVariant2 = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                    PROJECT_ACCESSION, "1", 1000L, "T", "G");
        SubmittedVariantEntity submittedVariantEntity2 = createSubmittedVariantEntity(2L, submittedVariant2);
        //Same assembly, contig, start but different type
        SubmittedVariant submittedVariantINS = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                      PROJECT_ACCESSION, "1", 1000L, "", "A");
        SubmittedVariantEntity submittedVariantEntityINS = createSubmittedVariantEntity(3L, submittedVariantINS);
        SubmittedVariant submittedVariantDEL = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                      PROJECT_ACCESSION, "1", 1000L, "T", "");
        SubmittedVariantEntity submittedVariantEntityDEL = createSubmittedVariantEntity(4L, submittedVariantDEL);
        //Different assembly, contig and start
        SubmittedVariant submittedVariant3 = createSubmittedVariant(inputParameters.getAssemblyAccession(), 3000,
                                                                    PROJECT_ACCESSION, "1", 3000L, "C", "G");
        SubmittedVariantEntity submittedVariantEntity3 = createSubmittedVariantEntity(5L, submittedVariant3);
        submittedVariantEntities.add(submittedVariantEntity1);
        submittedVariantEntities.add(submittedVariantEntity2);
        submittedVariantEntities.add(submittedVariantEntityINS);
        submittedVariantEntities.add(submittedVariantEntityDEL);
        submittedVariantEntities.add(submittedVariantEntity3);
        return submittedVariantEntities;
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(Long accession, SubmittedVariant submittedVariant) {
        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(accession, hash, submittedVariant, 1);
        return submittedVariantEntity;
    }

    private SubmittedVariant createSubmittedVariant(String referenceSequenceAccession, int taxonomyAccession,
                                                    String projectAccession, String contig, long start,
                                                    String referenceAllele, String alternateAllele) {
        return new SubmittedVariant(referenceSequenceAccession, taxonomyAccession, projectAccession, contig, start,
                                    referenceAllele, alternateAllele, null);
    }

    private void assertClusteredVariantsCreated() {
        MongoCollection<Document> collection = mongoTemplate.getCollection(CLUSTERED_VARIANT_COLLECTION);
        assertEquals(4, collection.countDocuments());
        List<Long> expectedAccessions = Arrays.asList(3000000000L, 3000000001L, 3000000002L, 3000000003L);
        assertGeneratedAccessions(CLUSTERED_VARIANT_COLLECTION, "accession", expectedAccessions);
    }

    private void assertGeneratedAccessions(String collectionName, String accessionField,
                                           List<Long> expectedAccessions) {
        List<Long> generatedAccessions = new ArrayList<>();
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
        FindIterable<Document> dbObjects = collection.find();
        for (Document document : dbObjects) {
            Long accessionId = (Long) document.get(accessionField);
            generatedAccessions.add(accessionId);
        }
        Collections.sort(generatedAccessions);
        assertEquals(expectedAccessions, generatedAccessions);
    }

    private void assertSubmittedVariantsUpdated() {
        assertTrue(allSubmittedVariantsClustered());
        List<Long> expectedAccessions = Arrays.asList(3000000000L, 3000000000L, 3000000001L, 3000000002L, 3000000003L);
        assertGeneratedAccessions(SUBMITTED_VARIANT_COLLECTION, "rs", expectedAccessions);
    }

    private boolean allSubmittedVariantsClustered() {
        MongoCollection<Document> collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (document.get("rs") == null){
                return false;
            }
        }
        return true;
    }

    @Test
    @DirtiesContext
    public void reuse_clustered_accession_if_provided() throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        long existingRs = 30L;
        String asm1 = "asm1";
        String asm2 = "asm2";

        ClusteredVariantEntity cve1 = createClusteredVariantEntity(asm1, existingRs);
        mongoTemplate.insert(cve1);
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // given a submitted variant that already has an RS=rs30
        SubmittedVariantEntity sveClustered = createSubmittedVariantEntity(asm1, existingRs, 50L);
        clusteringWriter.write(Collections.singletonList(sveClustered));
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // given a different submitted variant without an assigned RS (rs=null)
        SubmittedVariantEntity sveNonClustered = createSubmittedVariantEntity(asm2, null, 51L);
        clusteringWriter.write(Collections.singletonList(sveNonClustered));
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // given the same submitted variant without an assigned RS (rs=null), getOrCreate should not create another RS
        clusteringWriter.write(Collections.singletonList(sveNonClustered));
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long rs, Long ss) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, 1000, "project", "1", 100L, "T", "A", rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        return new SubmittedVariantEntity(ss, hash1, submittedClustered, 1);
    }

    private ClusteredVariantEntity createClusteredVariantEntity(String assembly, Long rs) {
        ClusteredVariant cv = new ClusteredVariant(assembly, 1000, "1", 100L, VariantType.SNV, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new ClusteredVariantEntity(rs, cvHash, cv, 1);
    }

    private DbsnpClusteredVariantEntity createDbsnpClusteredVariantEntity(String assembly, Long rs) {
        ClusteredVariant cv = new ClusteredVariant(assembly, 1000, "1", 100L, VariantType.SNV, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new DbsnpClusteredVariantEntity(rs, cvHash, cv, 1);
    }

    @Test
    @DirtiesContext
    public void reuse_dbsnp_clustered_accession_when_clustering_an_eva_submitted_variant() throws Exception {
        // given
        Long rs1 = 30L;

        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        ClusteredVariant cv1 = new ClusteredVariant("asm1", 1000, "1", 100L, VariantType.SNV, false, null);
        String cvHash1 = clusteredHashingFunction.apply(cv1);
        DbsnpClusteredVariantEntity cve1 = new DbsnpClusteredVariantEntity(rs1, cvHash1, cv1, 1);
        mongoTemplate.insert(cve1);

        SubmittedVariant submittedNonClustered = new SubmittedVariant("asm1", 1000, "project2", "1", 100L, "T", "A",
                                                                      null);
        SubmittedVariantEntity sveNonClustered = createSubmittedVariantEntity(51L, submittedNonClustered);
        mongoTemplate.insert(sveNonClustered);

        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // when
        clusteringWriter.write(Collections.singletonList(sveNonClustered));

        // then
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        SubmittedVariantEntity afterClustering = mongoTemplate.findOne(new Query(), SubmittedVariantEntity.class);
        assertEquals(rs1, afterClustering.getClusteredVariantAccession());
    }

    @Test
    @DirtiesContext
    public void merge_eva_clustered_accession() throws Exception {
        Long rs1 = 3000000000L;
        Long rs2 = 3100000000L;
        long ss1 = 50L;
        long ss2 = 51L;
        merge_clustered_accession(rs1, rs2, ss1, ss2, 0, 2, 0, 1);
        assertMergedInto(rs1, rs2, ss2);
    }

    @Test
    @DirtiesContext
    public void merge_eva_clustered_accession_reversed() throws Exception {
        Long rs1 = 3100000000L;
        Long rs2 = 3000000000L;
        long ss1 = 50L;
        long ss2 = 51L;
        merge_clustered_accession(rs1, rs2, ss1, ss2, 0, 2, 0, 1);
        assertMergedInto(rs2, rs1, ss1);
    }

    @Test
    @DirtiesContext
    public void merge_dbsnp_clustered_accession() throws Exception {
        Long rs1 = 30L;
        Long rs2 = 31L;
        long ss1 = 50L;
        long ss2 = 51L;
        merge_clustered_accession(rs1, rs2, ss1, ss2, 2, 0, 1, 0);
        assertMergedInto(rs1, rs2, ss2);
    }

    @Test
    @DirtiesContext
    public void merge_eva_into_dbsnp_clustered_accession() throws Exception {
        Long rs1 = 3000000000L;
        Long rs2 = 31L;
        long ss1 = 50L;
        long ss2 = 51L;
        merge_clustered_accession(rs1, rs2, ss1, ss2, 1, 1, 0, 1);
        assertMergedInto(rs1, rs2, ss2);
    }

    public void merge_clustered_accession(Long rs1, Long rs2,
                                          long ss1, Long ss2, int expectedDbsnpCve,
                                          int expectedCve,
                                          int expectedDbsnpCvOperations, int expectedCvOperations)
            throws Exception {
        // given
        String asm1 = "asm1";
        String asm2 = "asm2";
        assertDatabaseCounts(0, 0, 0, 0, 0, 0, 0, 0);

        mongoTemplate.insert(createClusteredVariantEntity(asm1, rs1), getClusteredTable(rs1));

        // rs2 will be merged into rs1, but the document will be kept to describe how rs1 maps into this second assembly
        mongoTemplate.insert(createClusteredVariantEntity(asm2, rs2), getClusteredTable(rs2));

        // ss1 in the old assembly, will be remapped to asm2 (see sve1Remapped below)
        mongoTemplate.insert(createSubmittedVariantEntity(asm1, rs1, ss1));

        // ss2 in the new assembly, will change its RS to rs1 when we realise rs1 and rs2 should be merged
        // because they both map to the same position in asm2
        mongoTemplate.insert(createSubmittedVariantEntity(asm2, rs2, ss2));

        assertDatabaseCounts(0, expectedDbsnpCve, 2, expectedCve, 0, 0, 0, 0);

        // when
        SubmittedVariantEntity sve1Remapped = createSubmittedVariantEntity(asm2, rs1, ss1);
        clusteringWriter.write(Collections.singletonList(sve1Remapped));

        // then
        assertDatabaseCounts(0, expectedDbsnpCve, 2, expectedCve, 0, expectedDbsnpCvOperations, 1, expectedCvOperations);

        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class);
        assertReferenceSequenceAccessionEqual(Sets.newTreeSet(asm1, asm2), submittedVariants);

        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertAssemblyAccessionEqual(Sets.newTreeSet(asm1, asm2), clusteredVariants);
    }

    private void assertMergedInto(Long mergedInto, Long originalAccession, Long updatedSubmittedVariant) {
        List<SubmittedVariantEntity> submittedVariants = mongoTemplate.findAll(SubmittedVariantEntity.class);
        assertClusteredVariantAccessionEqual(Sets.newTreeSet(mergedInto), submittedVariants);

        List<ClusteredVariantEntity> clusteredVariants = mongoTemplate.findAll(ClusteredVariantEntity.class);
        assertAccessionEqual(Sets.newTreeSet(mergedInto), clusteredVariants);

        ClusteredVariantOperationEntity clusteredOp =
                mongoTemplate.findOne(new Query(), ClusteredVariantOperationEntity.class);
        assertEquals(EventType.MERGED, clusteredOp.getEventType());
        assertEquals(originalAccession, clusteredOp.getAccession());
        assertEquals(mergedInto, clusteredOp.getMergedInto());

        SubmittedVariantOperationEntity submittedOp =
                mongoTemplate.findOne(new Query(), SubmittedVariantOperationEntity.class);
        assertEquals(EventType.UPDATED, submittedOp.getEventType());
        assertEquals(updatedSubmittedVariant, submittedOp.getAccession());
    }

    private String getClusteredTable(Long rs1) {
        return isEvaClusteredVariant(rs1) ? CLUSTERED_VARIANT_ENTITY : DBSNP_CLUSTERED_VARIANT_ENTITY;
    }

    private boolean isEvaClusteredVariant(Long rs1) {
        return rs1 >= EVA_CLUSTERED_VARIANT_RANGE_START;
    }

    private void assertDatabaseCounts(int dbsnpSve, int dbsnpCve, int sve, int cve, int dbsnpSvOperation, int dbsnpCvOperation,
                                      int svOperation, int cvOperation) {
        assertEquals(dbsnpSve, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(dbsnpCve, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(sve, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(cve, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(dbsnpSvOperation, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));
        assertEquals(dbsnpCvOperation, mongoTemplate.count(new Query(), DbsnpClusteredVariantOperationEntity.class));
        assertEquals(svOperation, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(cvOperation, mongoTemplate.count(new Query(), ClusteredVariantOperationEntity.class));
    }
}
