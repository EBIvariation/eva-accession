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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.util.StreamUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.rest.ClusteredVariantsRestController;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantsRestControllerTest {

    private static final String URL = "/v1/clustered-variants/";

    private static final long CLUSTERED_VARIANT_ACCESSION_1 = 1L;

    private static final long CLUSTERED_VARIANT_ACCESSION_2 = 2L;

    private static final long SUBMITTED_VARIANT_ACCESSION_1 = 11L;

    private static final long SUBMITTED_VARIANT_ACCESSION_2 = 12L;

    private static final long SUBMITTED_VARIANT_ACCESSION_3 = 13L;

    private static final int VERSION_1 = 1;

    private static final int VERSION_2 = 1;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository repository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private ClusteredVariantsRestController controller;

    @Autowired
    private TestRestTemplate testRestTemplate;

    private Iterable<DbsnpClusteredVariantEntity> generatedAccessions;

    private Iterable<DbsnpSubmittedVariantEntity> generatedSubmittedVariantAccessions;

    private DbsnpSubmittedVariantEntity submittedVariantEntity1;

    private DbsnpSubmittedVariantEntity submittedVariantEntity2;

    private DbsnpSubmittedVariantEntity submittedVariantEntity3;

    @Before
    public void setUp() {
        repository.deleteAll();

        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 1101, "CHROM1", 1234, VariantType.SNV, false);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC01", 1102, "CHROM1", 1234, VariantType.MNV, false);

        DbsnpClusteredVariantSummaryFunction function = new DbsnpClusteredVariantSummaryFunction();
        DbsnpClusteredVariantEntity entity1 = new DbsnpClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                              function.apply(variant1), variant1);
        DbsnpClusteredVariantEntity entity2 = new DbsnpClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                              function.apply(variant2), variant2);


        SubmittedVariant submittedVariant1 = new SubmittedVariant("ASMACC01", 1101, "PROJECT1", "CHROM1", 1234, "REF",
                                                                  "ALT", CLUSTERED_VARIANT_ACCESSION_1);
        SubmittedVariant submittedVariant2 = new SubmittedVariant("ASMACC01", 1102, "PROJECT1", "CHROM1", 1234, "REF",
                                                                  "ALT", CLUSTERED_VARIANT_ACCESSION_2);
        SubmittedVariant submittedVariant3 = new SubmittedVariant("ASMACC01", 1102, "PROJECT2", "CHROM1", 1234, "REF",
                                                                  "ALT", CLUSTERED_VARIANT_ACCESSION_2);

        DbsnpSubmittedVariantSummaryFunction submittedVariantSummaryFunction =
                new DbsnpSubmittedVariantSummaryFunction();
        submittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                submittedVariantSummaryFunction.apply(submittedVariant1),
                                                submittedVariant1, VERSION_1);
        submittedVariantEntity2 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                submittedVariantSummaryFunction.apply(submittedVariant2),
                                                submittedVariant2, VERSION_1);
        submittedVariantEntity3 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_3,
                                                submittedVariantSummaryFunction.apply(submittedVariant3),
                                                submittedVariant2, VERSION_2);

        // No new dbSNP accessions can be generated, so the variants can only be stored directly using a repository
        // TODO When the support for new EVA accessions is implemented, this could be changed
        // In order to do so, replicate the structure of {@link SubmittedVariantsRestControllerTest}
        generatedAccessions = repository.save(Arrays.asList(entity1, entity2));
        generatedSubmittedVariantAccessions = submittedVariantRepository.save(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2, submittedVariantEntity3));
    }

    @After
    public void tearDown() {
        repository.deleteAll();
        submittedVariantRepository.deleteAll();
    }

    @Test
    public void testGetVariantsRestApi() {
        String identifiers = StreamUtils.createStreamFromIterator(generatedAccessions.iterator())
                .map(acc -> acc.getAccession().toString()).collect(Collectors.joining(","));
        String getVariantsUrl = URL + identifiers;

        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                          new ParameterizedTypeReference<
                                                  List<
                                                          AccessionResponseDTO<
                                                                  ClusteredVariant,
                                                                  IClusteredVariant,
                                                                  String,
                                                                  Long>>>() {
                                          });
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(2, getVariantsResponse.getBody().size());
        assertDefaultFlags(getVariantsResponse.getBody());
    }

    @Test
    public void testGetVariantsController() {
        List<Long> identifiers = StreamUtils.createStreamFromIterator(generatedAccessions.iterator())
                .map(acc -> acc.getAccession()).collect(Collectors.toList());

        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse =
                controller.get(identifiers);

        assertEquals(2, getVariantsResponse.size());
        assertDefaultFlags(getVariantsResponse);
    }

    @Test
    public void testGetSubmittedVariants() {
        getSubmittedVariantsByClusteredVariantIds(Collections.singletonList(CLUSTERED_VARIANT_ACCESSION_1),
                                                  Collections.singletonList(submittedVariantEntity1));
        getSubmittedVariantsByClusteredVariantIds(Collections.singletonList(CLUSTERED_VARIANT_ACCESSION_2),
                                                  Arrays.asList(submittedVariantEntity2, submittedVariantEntity3));
        getSubmittedVariantsByClusteredVariantIds(Arrays.asList(CLUSTERED_VARIANT_ACCESSION_1,
                                                                CLUSTERED_VARIANT_ACCESSION_2),
                                                  Arrays.asList(submittedVariantEntity1, submittedVariantEntity2,
                                                                submittedVariantEntity3));
    }

    private void getSubmittedVariantsByClusteredVariantIds(List<Long> clusteredVariantIds,
                                                           List<DbsnpSubmittedVariantEntity> expectedSubmittedVariants)
    {
        List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantsResponse =
                controller.getSubmittedVariants(clusteredVariantIds);

        assertEquals(expectedSubmittedVariants.size(), getVariantsResponse.size());

        Set<Long> retrievedAccessions = getVariantsResponse.stream().map(AccessionResponseDTO::getAccession).collect(
                Collectors.toSet());

        assertTrue(expectedSubmittedVariants.stream().allMatch(
                submittedVariant -> retrievedAccessions.contains(submittedVariant.getAccession())));

        Set<SubmittedVariant> retrievedSubmitedVariants = getVariantsResponse.stream().map(
                AccessionResponseDTO::getData).collect(Collectors.toSet());

        assertTrue(expectedSubmittedVariants.stream().allMatch(
                submittedVariant -> retrievedSubmitedVariants.contains(submittedVariant.getModel())));

    }

    private void assertDefaultFlags(
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> body) {
        ClusteredVariant variant = body.get(0).getData();
        assertEquals(IClusteredVariant.DEFAULT_VALIDATED, variant.isValidated());
    }
}
