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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.ContiguousIdBlocksDataSourceConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.commons.mongodb.readers.MongoDbCursorItemReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_CANDIDATES_READER;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:clustering-pipeline-test.properties")
@ContextConfiguration(classes = {RSMergeAndSplitCandidatesReaderConfiguration.class, MongoTestConfiguration.class,
        BatchTestConfiguration.class, ContiguousIdBlocksDataSourceConfiguration.class})
public class RSMergeAndSplitCandidatesReaderTest extends MongoTestContainerHelper {

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final String SUBMITTED_VARIANT_OPERATION_COLLECTION = "submittedVariantOperationEntity";

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(RS_SPLIT_CANDIDATES_READER)
    private MongoDbCursorItemReader<SubmittedVariantOperationEntity> rsSplitCandidatesReader;

    @Autowired
    @Qualifier(RS_MERGE_CANDIDATES_READER)
    private MongoDbCursorItemReader<SubmittedVariantOperationEntity> rsMergeCandidatesReader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void readAllRSSplitCandidates() throws Exception {
        createSplitCandidateEntries();
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity submittedVariantOperationEntity;
        ExecutionContext executionContext = new ExecutionContext();
        this.rsSplitCandidatesReader.open(executionContext);
        while ((submittedVariantOperationEntity = rsSplitCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(submittedVariantOperationEntity);
        }
        //Ensure that the reader reads the two operations created during the setup routine
        assertEquals(2, submittedVariantOperationEntities.size());
    }

    @Test
    public void readAllRSMergeCandidates() throws Exception {
        createMergeCandidateEntries();
        List<SubmittedVariantOperationEntity> submittedVariantOperationEntities = new ArrayList<>();
        SubmittedVariantOperationEntity submittedVariantOperationEntity;
        ExecutionContext executionContext = new ExecutionContext();
        this.rsMergeCandidatesReader.open(executionContext);
        while ((submittedVariantOperationEntity = rsMergeCandidatesReader.read()) != null) {
            submittedVariantOperationEntities.add(submittedVariantOperationEntity);
        }
        //Ensure that the reader reads the two operations created during the setup routine
        assertEquals(2, submittedVariantOperationEntities.size());
    }

    private SubmittedVariantEntity createSSWithLocus(Long ssAccession, Long rsAccession, Long start, String reference,
                                                     String alternate) {
        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, ASSEMBLY, 60711,
                "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false, false,
                false, 1);
    }

    private void createSplitCandidateEntries() {
        //Candidates for split are entries with same RS but different locus
        SubmittedVariantInactiveEntity ss1 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(1L, 1L, 100L, "C", "T"));
        SubmittedVariantInactiveEntity ss2 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(2L, 1L, 101L, "G", "A"));
        SubmittedVariantInactiveEntity ss3 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(3L, 1L, 102L, "T", "C"));
        SubmittedVariantOperationEntity splitOperation1 = new SubmittedVariantOperationEntity();
        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        splitOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                ss1.getAccession(), null, "Hash mismatch with rs1",
                Arrays.asList(ss1, ss2, ss3));
        splitOperation1.setId(ClusteringWriter.getSplitCandidateId(splitOperation1));

        SubmittedVariantInactiveEntity ss4 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(4L, 2L, 103L, "C", "T"));
        SubmittedVariantInactiveEntity ss5 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(5L, 2L, 104L, "G", "A"));
        SubmittedVariantInactiveEntity ss6 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(6L, 2L, 105L, "T", "C"));
        SubmittedVariantOperationEntity splitOperation2 = new SubmittedVariantOperationEntity();
        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        splitOperation2.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                ss4.getAccession(),
                null, "Hash mismatch with rs2",
                Arrays.asList(ss4, ss5, ss6));
        splitOperation2.setId(ClusteringWriter.getSplitCandidateId(splitOperation2));

        mongoTemplate.insert(splitOperation1, SUBMITTED_VARIANT_OPERATION_COLLECTION);
        mongoTemplate.insert(splitOperation2, SUBMITTED_VARIANT_OPERATION_COLLECTION);
    }

    private void createMergeCandidateEntries() {
        //Candidates for merge are entries with same locus but different RS
        SubmittedVariantInactiveEntity ss1 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(1L, 1L, 100L, "C", "T"));
        SubmittedVariantInactiveEntity ss2 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(2L, 2L, 100L, "C", "A"));
        SubmittedVariantInactiveEntity ss3 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(3L, 3L, 100L, "C", "G"));
        SubmittedVariantOperationEntity mergeOperation1 = new SubmittedVariantOperationEntity();
        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        mergeOperation1.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                ss1.getAccession(), null, "Different RS with matching loci",
                Arrays.asList(ss1, ss2, ss3));
        mergeOperation1.setId(ClusteringWriter.getMergeCandidateId(mergeOperation1));

        SubmittedVariantInactiveEntity ss4 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(4L, 4L, 103L, "A", "C"));
        SubmittedVariantInactiveEntity ss5 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(5L, 5L, 103L, "A", "T"));
        SubmittedVariantInactiveEntity ss6 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(6L, 6L, 103L, "A", "G"));
        SubmittedVariantOperationEntity mergeOperation2 = new SubmittedVariantOperationEntity();
        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        mergeOperation2.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                ss4.getAccession(),
                null, "Different RS with matching loci",
                Arrays.asList(ss4, ss5, ss6));
        mergeOperation2.setId(ClusteringWriter.getMergeCandidateId(mergeOperation2));

        mongoTemplate.insert(mergeOperation1, SUBMITTED_VARIANT_OPERATION_COLLECTION);
        mongoTemplate.insert(mergeOperation2, SUBMITTED_VARIANT_OPERATION_COLLECTION);
    }
}