/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.ws;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.HistoryEventDTO;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.accession.ws.dto.VariantHistory;
import uk.ac.ebi.eva.accession.ws.rest.ClusteredVariantsRestController;
import uk.ac.ebi.eva.accession.ws.test.MongoTestConfiguration;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({MongoTestConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantHistoryEndPointTest extends MongoTestContainerHelper {

    private static final String URL = "/v1/clustered-variants/%s/history";

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private ClusteredVariantsRestController restController;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    private static class ClusteredVariantType extends ParameterizedTypeReference<
            VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> {
    }

    @Test
    @DirtiesContext
    public void testVariantHistorySingleRSSplitIntoMultiple1() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).loadAll("/test-data/splitOneRsIntoMultiple.json");

        long fetchHistoryOfRS = 1L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS, ContigNamingConvention.INSDC);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants =
                variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(3, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(1L)));
        //Variant with RS 1 present in both ASM1 and ASM2
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM1", "ASM2")
                .contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.RS_SPLIT)));
        // Every split operation from RS 1
        assertTrue(allOperations.stream().allMatch(o -> o.getAccession().equals(1L)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(3000000000L, 3000000001L, 3000000002L)
                .contains(o.getSplitInto())));
    }

    @Test
    @DirtiesContext
    public void testVariantHistoryRSMerge() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).loadAll("/test-data/rsMerge.Json");

        long fetchHistoryOfRS = 1L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS, ContigNamingConvention.INSDC);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants =
                variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(1, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(1L)));
        //Variant present in both ASM1 and ASM2
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM1", "ASM2")
                .contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.MERGED)));
        // Operation RS 2 merged into 1
        assertEquals(2L, allOperations.get(0).getAccession().longValue());
        assertEquals(1L, allOperations.get(0).getMergedInto().longValue());
    }

    @Test
    @DirtiesContext
    public void testVariantHistorySubsequentRSSplit() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).loadAll("/test-data/subsequentRsSplit.json");

        long fetchHistoryOfRS = 3000000000L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS, ContigNamingConvention.INSDC);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants =
                variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(2, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(3000000000L)));
        //Variant present in both ASM2 and ASM3
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM2", "ASM3")
                .contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.RS_SPLIT)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(1L, 3000000000L).contains(o.getAccession())));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(3000000000L, 3000000001L)
                .contains(o.getSplitInto())));
    }

    @Test
    @DirtiesContext
    public void testVariantHistorySubsequentRSMerge() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).loadAll("/test-data/subsequentRsMerge.json");

        long fetchHistoryOfRS = 9L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS, ContigNamingConvention.INSDC);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants =
                variantHistory.getVariants();
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
        new MongoTestDataLoader(mongoTemplate, resourceLoader).loadAll("/test-data/mixOfSplitAndMerge.json");

        long fetchHistoryOfRS = 3000000000L;
        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> response =
                restController.getVariantHistory(fetchHistoryOfRS, ContigNamingConvention.INSDC);
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory = response.getBody();
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants =
                variantHistory.getVariants();
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
    public void testVariantHistoryServiceEndPoint() {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).loadAll("/test-data/rsSplitsEndpoint.json");

        int fetchHistoryOfRS = 1;
        String getVariantsUrl = String.format(URL, fetchHistoryOfRS);

        ResponseEntity<VariantHistory<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse =
                testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new ClusteredVariantType());
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        VariantHistory<ClusteredVariant, IClusteredVariant, String, Long> variantHistory =
                getVariantsResponse.getBody();

        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> allVariants =
                variantHistory.getVariants();
        List<HistoryEventDTO<Long, ClusteredVariant>> allOperations = variantHistory.getOperations();

        assertEquals(2, allVariants.size());
        assertEquals(3, allOperations.size());
        assertTrue(allVariants.stream().allMatch(v -> v.getAccession().equals(1L)));
        //Variant with RS 1 present in both ASM1 and ASM2
        assertTrue(allVariants.stream().allMatch(v -> Arrays.asList("ASM1", "ASM2")
                .contains(v.getData().getAssemblyAccession())));
        assertTrue(allOperations.stream().allMatch(o -> o.getType().equals(EventType.RS_SPLIT)));
        // Every split operation from RS 1
        assertTrue(allOperations.stream().allMatch(o -> o.getAccession().equals(1L)));
        assertTrue(allOperations.stream().allMatch(o -> Arrays.asList(3000000000L, 3000000001L, 3000000002L)
                .contains(o.getSplitInto())));
    }
}