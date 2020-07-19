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

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * This class handles the simplest scenarios of ClusteringWriter.
 *
 * The scenarios tested here are those about issuing new clustered variant accessions (RSs). No reusing existing RSs,
 * no RSs provided with the submitted variant being clustered, no RS merging. Other test classes in this folder take
 * care of those scenarios.
 */
@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class IssueAccessionClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    private static final String SUBMITTED_VARIANT_OPERATION_COLLECTION = "submittedVariantOperationEntity";

    private static final String PROJECT_ACCESSION = "projectId_1";

    public static final long EVA_CLUSTERED_VARIANT_RANGE_START = 3000000000L;

    public static final long EVA_SUBMITTED_VARIANT_RANGE_START = 5000000000L;

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
        clusteringWriter = new ClusteringWriter(mongoTemplate, clusteredVariantAccessioningService,
                                                EVA_SUBMITTED_VARIANT_RANGE_START, EVA_CLUSTERED_VARIANT_RANGE_START);
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    @DirtiesContext
    public void writer() throws Exception {
        List<SubmittedVariantEntity> submittedVariantEntities = createSubmittedVariantEntities();
        clusteringWriter.write(submittedVariantEntities);
        assertClusteredVariantsCreated();
        assertSubmittedVariantsUpdated();
        assertSubmittedVariantsOperationInserted();
    }

    private List<SubmittedVariantEntity> createSubmittedVariantEntities() {
        List<SubmittedVariantEntity> submittedVariantEntities = new ArrayList<>();
        SubmittedVariant submittedVariant1 = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                    PROJECT_ACCESSION, "1", 1000L, "T", "A");
        SubmittedVariantEntity submittedVariantEntity1 = createSubmittedVariantEntity(5000000001L, submittedVariant1);
        //Different alleles
        SubmittedVariant submittedVariant2 = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                    PROJECT_ACCESSION, "1", 1000L, "T", "G");
        SubmittedVariantEntity submittedVariantEntity2 = createSubmittedVariantEntity(5000000002L, submittedVariant2);
        //Same assembly, contig, start but different type
        SubmittedVariant submittedVariantINS = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                      PROJECT_ACCESSION, "1", 1000L, "", "A");
        SubmittedVariantEntity submittedVariantEntityINS = createSubmittedVariantEntity(5000000003L, submittedVariantINS);
        SubmittedVariant submittedVariantDEL = createSubmittedVariant(inputParameters.getAssemblyAccession(), 1000,
                                                                      PROJECT_ACCESSION, "1", 1000L, "T", "");
        SubmittedVariantEntity submittedVariantEntityDEL = createSubmittedVariantEntity(5000000004L, submittedVariantDEL);
        //Different assembly, contig and start
        SubmittedVariant submittedVariant3 = createSubmittedVariant(inputParameters.getAssemblyAccession(), 3000,
                                                                    PROJECT_ACCESSION, "1", 3000L, "C", "G");
        SubmittedVariantEntity submittedVariantEntity3 = createSubmittedVariantEntity(5000000005L, submittedVariant3);
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

    private void assertSubmittedVariantsOperationInserted(){
        MongoCollection<Document> collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_OPERATION_COLLECTION);
        assertEquals(5, collection.countDocuments());
        List<Long> expectedAccessions = Arrays.asList(5000000001L, 5000000002L, 5000000003L, 5000000004L, 5000000005L);
        assertGeneratedAccessions(SUBMITTED_VARIANT_OPERATION_COLLECTION, "accession", expectedAccessions);
    }
}
