/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.ws;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.HistoryEventDTO;
import uk.ac.ebi.eva.accession.clustering.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.dto.VariantHistory;
import uk.ac.ebi.eva.accession.ws.rest.ClusteredVariantsRestController;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class,
        RSMergeAndSplitWriterConfiguration.class, ListenersConfiguration.class, InputParametersConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantHistoryEndPointTest {

    private static final String URL = "/v1/clustered-variants/%s/history";

    private static final String DBSNP_SUBMITTED_VARIANT_ENTITY = "dbsnpSubmittedVariantEntity";

    private static final String SUBMITTED_VARIANT_ENTITY = "submittedVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private static final String CLUSTERED_VARIANT_ENTITY = "clusteredVariantEntity";

    private static final String CLUSTERED_VARIANT_OPERATION_ENTITY = "clusteredVariantOperationEntity";

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(RS_SPLIT_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsSplitWriter;

    @Autowired
    @Qualifier(RS_MERGE_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsMergeWriter;

    @Autowired
    private ClusteredVariantsRestController restController;

    @Before
    public void setUp() {
        mongoTemplate.dropCollection(DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY);
        mongoTemplate.dropCollection(CLUSTERED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(CLUSTERED_VARIANT_OPERATION_ENTITY);
    }

    @After
    public void tearDown() {
        mongoTemplate.dropCollection(DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY);
        mongoTemplate.dropCollection(CLUSTERED_VARIANT_ENTITY);
        mongoTemplate.dropCollection(CLUSTERED_VARIANT_OPERATION_ENTITY);
    }

    private static class ClusteredVariantType extends ParameterizedTypeReference<
            VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> {
    }

    @Test
    @DirtiesContext
    public void testVariantHistorySingleRSSplitIntoMultiple() throws Exception {
        //ASM1
        SubmittedVariantEntity asm1_ss1 = createSS("ASM1", 1L, 1L, 100L, "C", "T");
        mongoTemplate.insert(Collections.singletonList(asm1_ss1), DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.insert(Collections.singletonList(toClusteredVariantEntity(asm1_ss1)), DBSNP_CLUSTERED_VARIANT_ENTITY);

        //ASM2 (RS 1 will split into 3 different RS (3000000000L,3000000001L, 3000000002L)
        SubmittedVariantEntity asm2_ss1 = createSS("ASM2", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity asm2_ss2 = createSS("ASM2", 2L, 1L, 101L, "T", "A");
        SubmittedVariantEntity asm2_ss3 = createSS("ASM2", 3L, 1L, 102L, "C", "T");
        SubmittedVariantEntity asm2_ss4 = createSS("ASM2", 4L, 1L, 103L, "T", "A");

        List<SubmittedVariantEntity> submittedVariantEntities = Arrays.asList(asm2_ss1, asm2_ss2, asm2_ss3, asm2_ss4);
        mongoTemplate.insert(submittedVariantEntities, DBSNP_SUBMITTED_VARIANT_ENTITY);
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = submittedVariantEntities.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        mongoTemplate.insert(multipleEntriesWithSameRS, DBSNP_CLUSTERED_VARIANT_ENTITY);

        //Split function called
        splitRS(Arrays.asList(asm2_ss1, asm2_ss2, asm2_ss3, asm2_ss4));

        long fetchHistoryOfRS = 1L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants = variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(3, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(1L)));
        //Variant with RS 1 present in both ASM1 and ASM2
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM1", "ASM2").contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.RS_SPLIT)));
        // Every split operation from RS 1
        assertTrue(allOperations.stream().allMatch(o -> o.getAccession().equals(1L)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(3000000000L, 3000000001L, 3000000002L).contains(o.getSplitInto())));
    }

    @Test
    @DirtiesContext
    public void testVariantHistoryRSMerge() throws Exception {
        //ASM1
        SubmittedVariantEntity asm1_ss1 = createSS("ASM1", 1L, 1L, 100L, "C", "T");
        mongoTemplate.insert(asm1_ss1, DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.insert(Collections.singletonList(toClusteredVariantEntity(asm1_ss1)), DBSNP_CLUSTERED_VARIANT_ENTITY);

        //ASM2 (RS 2 will be merged into RS 1)
        SubmittedVariantEntity asm2_ss1 = createSS("ASM2", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity asm2_ss2 = createSS("ASM2", 2L, 2L, 100L, "C", "A");

        List<SubmittedVariantEntity> submittedVariantEntities = Arrays.asList(asm2_ss1, asm2_ss2);
        mongoTemplate.insert(submittedVariantEntities, DBSNP_SUBMITTED_VARIANT_ENTITY);

        //Merge
        mergeRS(Arrays.asList(asm2_ss1, asm2_ss2));

        long fetchHistoryOfRS = 1L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants = variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(1, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(1L)));
        //Variant present in both ASM1 and ASM2
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM1", "ASM2").contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.MERGED)));
        // Operation RS 2 merged into 1
        assertEquals(2L, allOperations.get(0).getAccession().longValue());
        assertEquals(1L, allOperations.get(0).getMergedInto().longValue());
    }

    @Test
    @DirtiesContext
    public void testVariantHistorySubsequentRSSplit() throws Exception {
        //ASM1
        SubmittedVariantEntity asm1_ss1 = createSS("ASM1", 1L, 1L, 100L, "C", "T");
        mongoTemplate.insert(Collections.singletonList(asm1_ss1), DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.insert(Collections.singletonList(toClusteredVariantEntity(asm1_ss1)), DBSNP_CLUSTERED_VARIANT_ENTITY);

        //ASM2 (RS 1 will split into RS 3000000000)
        SubmittedVariantEntity asm2_ss1 = createSS("ASM2", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity asm2_ss2 = createSS("ASM2", 2L, 1L, 101L, "T", "A");

        List<SubmittedVariantEntity> submittedVariantEntitiesAsm2 = Arrays.asList(asm2_ss1, asm2_ss2);
        mongoTemplate.insert(submittedVariantEntitiesAsm2, DBSNP_SUBMITTED_VARIANT_ENTITY);
        List<ClusteredVariantEntity> multipleEntriesWithSameRSAsm2 = submittedVariantEntitiesAsm2.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        mongoTemplate.insert(multipleEntriesWithSameRSAsm2, DBSNP_CLUSTERED_VARIANT_ENTITY);
        //SPLIT
        splitRS(Arrays.asList(asm2_ss1, asm2_ss2));

        //ASM3 (RS 3000000000 will split into 3000000001)
        SubmittedVariantEntity asm3_ss1 = createSS("ASM3", 2L, 3000000000L, 101L, "T", "A");
        SubmittedVariantEntity asm3_ss2 = createSS("ASM3", 3L, 3000000000L, 102L, "T", "A");

        List<SubmittedVariantEntity> submittedVariantEntitiesAsm3 = Arrays.asList(asm3_ss1, asm3_ss2);
        mongoTemplate.insert(submittedVariantEntitiesAsm3, DBSNP_SUBMITTED_VARIANT_ENTITY);
        List<ClusteredVariantEntity> multipleEntriesWithSameRSAsm3 = submittedVariantEntitiesAsm3.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        mongoTemplate.insert(multipleEntriesWithSameRSAsm3, DBSNP_CLUSTERED_VARIANT_ENTITY);
        //SPLIT
        splitRS(Arrays.asList(asm3_ss1, asm3_ss2));


        long fetchHistoryOfRS = 3000000000L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants = variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(2, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(3000000000L)));
        //Variant present in both ASM2 and ASM3
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM2", "ASM3").contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.RS_SPLIT)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(1L, 3000000000L).contains(o.getAccession())));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(3000000000L, 3000000001L).contains(o.getSplitInto())));
    }

    @Test
    @DirtiesContext
    public void testVariantHistorySubsequentRSMerge() throws Exception {
        //ASM1
        SubmittedVariantEntity asm1_ss1 = createSS("ASM1", 1L, 10L, 100L, "C", "T");
        mongoTemplate.insert(asm1_ss1, DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.insert(Collections.singletonList(toClusteredVariantEntity(asm1_ss1)), DBSNP_CLUSTERED_VARIANT_ENTITY);

        //ASM2 (RS 10 will merge into 9)
        SubmittedVariantEntity asm2_ss1 = createSS("ASM2", 1L, 10L, 100L, "C", "T");
        SubmittedVariantEntity asm2_ss2 = createSS("ASM2", 2L, 9L, 100L, "C", "A");

        List<SubmittedVariantEntity> submittedVariantEntitiesAsm2 = Arrays.asList(asm2_ss1, asm2_ss2);
        mongoTemplate.insert(submittedVariantEntitiesAsm2, DBSNP_SUBMITTED_VARIANT_ENTITY);
        //Merge
        mergeRS(Arrays.asList(asm2_ss1, asm2_ss2));

        //ASM2 (RS 9 will merge into 8)
        SubmittedVariantEntity asm3_ss1 = createSS("ASM3", 1L, 9L, 100L, "C", "T");
        SubmittedVariantEntity asm3_ss2 = createSS("ASM3", 3L, 8L, 100L, "C", "A");

        List<SubmittedVariantEntity> submittedVariantEntitiesAsm3 = Arrays.asList(asm3_ss1, asm3_ss2);
        mongoTemplate.insert(submittedVariantEntitiesAsm3, DBSNP_SUBMITTED_VARIANT_ENTITY);
        //Merge
        mergeRS(Arrays.asList(asm3_ss1, asm3_ss2));

        long fetchHistoryOfRS = 9L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants = variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(1, allVariants.size());
        assertEquals(2, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(9L)));
        //Variant present only in ASM2 (not present in ASM3 as it will be deleted after merging)
        assertTrue(allVariants.stream().allMatch(v -> v.getData().getAssemblyAccession().equals("ASM2")));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.MERGED)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(10L, 9L).contains(o.getAccession())));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(9L, 8L).contains(o.getMergedInto())));
    }

    @Test
    @DirtiesContext
    public void testVariantHistoryMixOfSplitAndMerge() throws Exception {
        //ASM1
        SubmittedVariantEntity asm1_ss1 = createSS("ASM1", 1L, 1L, 100L, "C", "T");
        mongoTemplate.insert(Collections.singletonList(asm1_ss1), DBSNP_SUBMITTED_VARIANT_ENTITY);
        mongoTemplate.insert(Collections.singletonList(toClusteredVariantEntity(asm1_ss1)), DBSNP_CLUSTERED_VARIANT_ENTITY);

        //ASM2 (RS 1 will split into RS 3000000000)
        SubmittedVariantEntity asm2_ss1 = createSS("ASM2", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity asm2_ss2 = createSS("ASM2", 2L, 1L, 101L, "T", "A");

        List<SubmittedVariantEntity> submittedVariantEntitiesAsm2 = Arrays.asList(asm2_ss1, asm2_ss2);
        mongoTemplate.insert(submittedVariantEntitiesAsm2, DBSNP_SUBMITTED_VARIANT_ENTITY);
        List<ClusteredVariantEntity> multipleEntriesWithSameRSAsm2 = submittedVariantEntitiesAsm2.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        mongoTemplate.insert(multipleEntriesWithSameRSAsm2, DBSNP_CLUSTERED_VARIANT_ENTITY);
        //SPLIT
        splitRS(Arrays.asList(asm2_ss1, asm2_ss2));

        //ASM3 (RS 3000000000 will merge into 8)
        SubmittedVariantEntity asm3_ss1 = createSS("ASM3", 1L, 3000000000L, 101L, "C", "T");
        SubmittedVariantEntity asm3_ss2 = createSS("ASM3", 3L, 8L, 101L, "C", "A");

        List<SubmittedVariantEntity> submittedVariantEntitiesAsm3 = Arrays.asList(asm3_ss1, asm3_ss2);
        mongoTemplate.insert(submittedVariantEntitiesAsm3, DBSNP_SUBMITTED_VARIANT_ENTITY);
        //Merge
        mergeRS(Arrays.asList(asm3_ss1, asm3_ss2));

        //Fetch History
        long fetchHistoryOfRS = 3000000000L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants = variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        // Assert Data
        assertEquals(1, allVariants.size());
        assertEquals(2, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(3000000000L)));
        //Variant present only in ASM2 (not present in ASM3 as it will be deleted after merging)
        assertTrue(allVariants.stream().allMatch(v -> v.getData().getAssemblyAccession().equals("ASM2")));
        assertEquals(EventType.RS_SPLIT, allOperations.get(0).getType());
        assertEquals(1L, allOperations.get(0).getAccession().longValue());
        assertEquals(3000000000L, allOperations.get(0).getSplitInto().longValue());
        assertEquals(EventType.MERGED, allOperations.get(1).getType());
        assertEquals(3000000000L, allOperations.get(1).getAccession().longValue());
        assertEquals(8L, allOperations.get(1).getMergedInto().longValue());
    }

    @Test
    @DirtiesContext
    public void testVariantHistoryServiceEndPoint() throws Exception {
        //ASM1
        SubmittedVariantEntity asm1_ss1 = createSS("ASM1", 1L, 1L, 100L, "C", "T");
        splitRS(Collections.singletonList(asm1_ss1));

        //ASM2
        SubmittedVariantEntity asm2_ss1 = createSS("ASM2", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity asm2_ss2 = createSS("ASM2", 2L, 1L, 101L, "T", "A");
        SubmittedVariantEntity asm2_ss3 = createSS("ASM2", 3L, 1L, 102L, "C", "T");
        SubmittedVariantEntity asm2_ss4 = createSS("ASM2", 4L, 1L, 103L, "T", "A");
        splitRS(Arrays.asList(asm2_ss1, asm2_ss2, asm2_ss3, asm2_ss4));

        int fetchHistoryOfRS = 1;
        String getVariantsUrl = String.format(URL, fetchHistoryOfRS);

        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse =
                testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new ClusteredVariantType());
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory =
                getVariantsResponse.getBody();

        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants = variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(3, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(1L)));
        //Variant with RS 1 present in both ASM1 and ASM2
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM1", "ASM2").contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.RS_SPLIT)));
        // Every split operation from RS 1
        assertTrue(allOperations.stream().allMatch(o -> o.getAccession().equals(1L)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(3000000000L, 3000000001L, 3000000002L).contains(o.getSplitInto())));
    }

    private void mergeRS(List<SubmittedVariantEntity> submittedVariantEntities) throws Exception {
        SubmittedVariantOperationEntity mergeOperation = new SubmittedVariantOperationEntity();
        mergeOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                submittedVariantEntities.get(0).getAccession(), null, "Different RS with matching loci",
                submittedVariantEntities.stream().map(SubmittedVariantInactiveEntity::new)
                        .collect(Collectors.toList()));

        rsMergeWriter.write(Collections.singletonList(mergeOperation));
    }

    private void splitRS(List<SubmittedVariantEntity> submittedVariantEntities) throws Exception {
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                submittedVariantEntities.get(0).getAccession(), "Hash mismatch with " +
                        submittedVariantEntities.get(0).getClusteredVariantAccession(),
                submittedVariantEntities.stream().map(SubmittedVariantInactiveEntity::new)
                        .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));
    }

    private SubmittedVariantEntity createSS(String assembly, Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate) {
        Function<ISubmittedVariant, String> hashingFunction = new SubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        SubmittedVariant temp = new SubmittedVariant(assembly, 60711, "PRJ1", "chr1", start, reference, alternate,
                rsAccession);
        return new SubmittedVariantEntity(ssAccession, hashingFunction.apply(temp), temp, 1);
    }

    private ClusteredVariantEntity toClusteredVariantEntity(SubmittedVariantEntity submittedVariantEntity) {
        return new ClusteredVariantEntity(submittedVariantEntity.getClusteredVariantAccession(),
                getClusteredVariantHash(submittedVariantEntity),
                toClusteredVariant(submittedVariantEntity));
    }

    private ClusteredVariant toClusteredVariant(ISubmittedVariant submittedVariant) {
        return new ClusteredVariant(submittedVariant.getReferenceSequenceAccession(),
                submittedVariant.getTaxonomyAccession(),
                submittedVariant.getContig(),
                submittedVariant.getStart(),
                VariantClassifier.getVariantClassification(submittedVariant.getReferenceAllele(),
                        submittedVariant.getAlternateAllele()),
                submittedVariant.isValidated(),
                submittedVariant.getCreatedDate());
    }

    protected String getClusteredVariantHash(ISubmittedVariant submittedVariant) {
        Function<IClusteredVariant, String> clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        ClusteredVariant clusteredVariant = toClusteredVariant(submittedVariant);
        return clusteredHashingFunction.apply(clusteredVariant);
    }
}