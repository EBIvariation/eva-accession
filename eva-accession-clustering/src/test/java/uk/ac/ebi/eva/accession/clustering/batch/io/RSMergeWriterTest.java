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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;

import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:merge-split-test.properties")
@ContextConfiguration(classes = {RSMergeAndSplitCandidatesReaderConfiguration.class, RSMergeWriterConfiguration.class,
        MongoTestConfiguration.class, BatchTestConfiguration.class})
public class RSMergeWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final String DBSNP_CLUSTERED_VARIANT_COLLECTION = "dbsnpClusteredVariantEntity";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String DBSNP_SUBMITTED_VARIANT_COLLECTION = "dbsnpSubmittedVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    private static final String SUBMITTED_VARIANT_OPERATION_COLLECTION = "submittedVariantOperationEntity";

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(RS_MERGE_CANDIDATES_READER)
    private ItemReader<SubmittedVariantOperationEntity> rsMergeCandidatesReader;

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

    private SubmittedVariantEntity ss1, ss2, ss4, ss5;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    private ClusteredVariantOperationRepository clusteredVariantOperationRepository;

    @Autowired
    private SubmittedVariantOperationRepository submittedVariantOperationRepository;

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate) {

        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, ASSEMBLY, 60711,
                                          "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false, false,
                                          false, 1);
    }

    private void createMergeCandidateEntries() {
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 4    4   chr1/100/SNV
         * 2    2   chr1/103/SNV
         * 5    5   chr1/103/SNV
         *
         * RS1, RS4 and RS2, RS5 are marked as merge candidate entries since these RS pairs share the same locus
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

        List<SubmittedVariantEntity> ssToInsertToDbsnpSVE = Arrays.asList(ss1, ss2);
        List<SubmittedVariantEntity> ssToInsertToSVE = Arrays.asList(ss4, ss5);
        List<ClusteredVariantEntity> rsToInsertToDbsnpCVE =
                ssToInsertToDbsnpSVE.stream().map(clusteringWriter::toClusteredVariantEntity)
                                    .collect(Collectors.toList());
        List<ClusteredVariantEntity> rsToInsertToCVE =
                ssToInsertToSVE.stream().map(clusteringWriter::toClusteredVariantEntity)
                               .collect(Collectors.toList());

        mongoTemplate.insert(ssToInsertToDbsnpSVE, DBSNP_SUBMITTED_VARIANT_COLLECTION);
        mongoTemplate.insert(ssToInsertToSVE, SUBMITTED_VARIANT_COLLECTION);
        mongoTemplate.insert(rsToInsertToDbsnpCVE, DBSNP_CLUSTERED_VARIANT_COLLECTION);
        mongoTemplate.insert(rsToInsertToCVE, CLUSTERED_VARIANT_COLLECTION);
        mongoTemplate.insert(mergeOperation1, SUBMITTED_VARIANT_OPERATION_COLLECTION);
        mongoTemplate.insert(mergeOperation2, SUBMITTED_VARIANT_OPERATION_COLLECTION);
    }


    private void cleanupDB() {
        mongoClient.dropDatabase(TEST_DB);
    }

    @Before
    public void setUp() {
        cleanupDB();
    }

    @After
    public void tearDown() {
        cleanupDB();
    }

    private void assertRSAssociatedWithSS(Long rsAccession, SubmittedVariantEntity submittedVariantEntity)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        assertEquals(rsAccession,
                     submittedVariantAccessioningService.getByAccession(submittedVariantEntity.getAccession()).getData()
                                                        .getClusteredVariantAccession());
    }

    @Test
    public void writeRSMerges() throws Exception {
        /*
         * SS   RS  LOC
         * 1    1   chr1/100/SNV
         * 4    4   chr1/100/SNV
         * 2    2   chr1/103/SNV
         * 5    5   chr1/103/SNV
         *
         * RS1, RS4 and RS2, RS5 are marked as merge candidate entries since these RS pairs share the same locus
         */
        createMergeCandidateEntries();
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity submittedVariantOperationEntity;
        while ((submittedVariantOperationEntity = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(submittedVariantOperationEntity);
        }

        //Before merge SS-RS associations: ss1-rs1, ss2-rs2, ss4-rs4, ss5-rs5
        assertRSAssociatedWithSS(1L, ss1);
        assertRSAssociatedWithSS(2L, ss2);
        assertRSAssociatedWithSS(4L, ss4);
        assertRSAssociatedWithSS(5L, ss5);

        //Perform merge
        rsMergeWriter.write(submittedVariantOperationEntities);

        //Ensure operations collections have appropriate entries to reflect rs4 merged to rs1 & rs5 merged to rs2
        assertEquals(Long.valueOf(1L), clusteredVariantOperationRepository.findAllByAccession(4L).get(0)
                                                                          .getMergedInto());
        assertEquals(Long.valueOf(2L), clusteredVariantOperationRepository.findAllByAccession(5L).get(0)
                                                                          .getMergedInto());
        assertEquals("Original rs4 was merged into rs1.",
                     submittedVariantOperationRepository.findAllByAccession(4L).get(0).getReason());
        assertEquals("Original rs5 was merged into rs2.",
                     submittedVariantOperationRepository.findAllByAccession(5L).get(0).getReason());

        //After merge SS-RS associations: ss1-rs1, ss2-rs2, ss4-rs1, ss5-rs2 since rs4 merged to rs1 & rs5 merged to rs2
        assertRSAssociatedWithSS(1L, ss1);
        assertRSAssociatedWithSS(2L, ss2);
        assertRSAssociatedWithSS(1L, ss4);
        assertRSAssociatedWithSS(2L, ss5);
    }
}