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
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantsRestControllerTest {

    private static final String URL = "/v1/clustered-variants/";

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_1 = 1L;

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_2 = 2L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_1 = 11L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_2 = 12L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_3 = 13L;

    private static final int VERSION_1 = 1;

    private static final int VERSION_2 = 1;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpRepository;

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

    private DbsnpClusteredVariantEntity clusteredVariantEntity1;

    private DbsnpClusteredVariantEntity clusteredVariantEntity2;

    @Before
    public void setUp() {
        setupDbSnpClusteredVariants();
        setupDbsnpSubmittedVariants();
    }

    private void setupDbSnpClusteredVariants() {
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 1101, "CHROM1", 1234, VariantType.SNV, false);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC01", 1102, "CHROM1", 1234, VariantType.MNV, false);

        DbsnpClusteredVariantSummaryFunction function = new DbsnpClusteredVariantSummaryFunction();
        clusteredVariantEntity1 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                                                                              function.apply(variant1), variant1);
        clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                                                                              function.apply(variant2), variant2);

        // No new dbSNP accessions can be generated, so the variants can only be stored directly using a repository
        // TODO When the support for new EVA accessions is implemented, this could be changed
        // In order to do so, replicate the structure of {@link SubmittedVariantsRestControllerTest}
        generatedAccessions = dbsnpRepository.save(Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity2));
    }

    private void setupDbsnpSubmittedVariants() {
        // one variant has no default flags, while the others have the default values
        SubmittedVariant submittedVariant1 = new SubmittedVariant("ASMACC01", 1101, "PROJECT1", "CHROM1", 1234, "REF",
                                                                  "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_1);
        SubmittedVariant submittedVariant2 = new SubmittedVariant("ASMACC01", 1102, "PROJECT1", "CHROM1", 1234, "REF",
                                                                  "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_2);
        SubmittedVariant submittedVariant3 = new SubmittedVariant("ASMACC01", 1102, "PROJECT2", "CHROM1", 1234, "REF",
                                                                  "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                                                                  !ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                  !ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH,
                                                                  !ISubmittedVariant.DEFAULT_ALLELES_MATCH,
                                                                  !ISubmittedVariant.DEFAULT_VALIDATED);

        DbsnpSubmittedVariantSummaryFunction submittedVariantSummaryFunction =
                new DbsnpSubmittedVariantSummaryFunction();
        submittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(DBSNP_SUBMITTED_VARIANT_ACCESSION_1,
                                                submittedVariantSummaryFunction.apply(submittedVariant1),
                                                submittedVariant1, VERSION_1);
        submittedVariantEntity2 =
                new DbsnpSubmittedVariantEntity(DBSNP_SUBMITTED_VARIANT_ACCESSION_2,
                                                submittedVariantSummaryFunction.apply(submittedVariant2),
                                                submittedVariant2, VERSION_1);
        submittedVariantEntity3 =
                new DbsnpSubmittedVariantEntity(DBSNP_SUBMITTED_VARIANT_ACCESSION_3,
                                                submittedVariantSummaryFunction.apply(submittedVariant3),
                                                submittedVariant2, VERSION_2);

        generatedSubmittedVariantAccessions = submittedVariantRepository.save(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2, submittedVariantEntity3));
    }

    @After
    public void tearDown() {
        dbsnpRepository.deleteAll();
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
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> wsResponseBody =
                getVariantsResponse.getBody();
        checkClusteredVariantsOutput(wsResponseBody);
    }

    private void checkClusteredVariantsOutput(
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse) {
        List<AccessionedDocument<IClusteredVariant, Long>> expectedVariants =
                Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity2);
        assertVariantsAreContainedInControllerResponse(getVariantsResponse,
                                                       expectedVariants,
                                                       ClusteredVariant::new);
    }

    @Test
    public void testGetVariantsController() {
        List<Long> identifiers = StreamUtils.createStreamFromIterator(generatedAccessions.iterator())
                .map(acc -> acc.getAccession()).collect(Collectors.toList());

        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse =
                controller.get(identifiers);

        checkClusteredVariantsOutput(getVariantsResponse);
    }

    @Test
    public void testGetSubmittedVariantsByClusteredVariantIds() {
        getAndCheckSubmittedVariantsByClusteredVariantIds(
                Collections.singletonList(DBSNP_CLUSTERED_VARIANT_ACCESSION_1),
                Collections.singletonList(submittedVariantEntity1));
        getAndCheckSubmittedVariantsByClusteredVariantIds(
                Collections.singletonList(DBSNP_CLUSTERED_VARIANT_ACCESSION_2),
                Arrays.asList(submittedVariantEntity2, submittedVariantEntity3));
        getAndCheckSubmittedVariantsByClusteredVariantIds(
                Arrays.asList(DBSNP_CLUSTERED_VARIANT_ACCESSION_1, DBSNP_CLUSTERED_VARIANT_ACCESSION_2),
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2, submittedVariantEntity3));
    }

    private void getAndCheckSubmittedVariantsByClusteredVariantIds(List<Long> clusteredVariantIds,
                                                                   List<AccessionedDocument<ISubmittedVariant, Long>>
                                                                   expectedSubmittedVariants)
    {
        List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantsResponse =
                controller.getSubmittedVariants(clusteredVariantIds);
        assertVariantsAreContainedInControllerResponse(getVariantsResponse,
                                                       expectedSubmittedVariants,
                                                       SubmittedVariant::new);
    }

    private <DTO, MODEL> void assertVariantsAreContainedInControllerResponse(
            List<AccessionResponseDTO<DTO, MODEL, String, Long>> getVariantsResponse,
            List<AccessionedDocument<MODEL, Long>> expectedVariants,
            Function<MODEL, DTO> modelToDto) {
        // check the number of expected objects
        assertEquals(expectedVariants.size(), getVariantsResponse.size());

        // check the accessiones returned by the service
        Set<Long> retrievedAccessions = getVariantsResponse.stream().map(AccessionResponseDTO::getAccession).collect(
                Collectors.toSet());
        assertTrue(expectedVariants.stream().allMatch(
                expectedVariant -> retrievedAccessions.contains(expectedVariant.getAccession())));

        // check the objects returned by the service
        Set<DTO> variantsReturnedByController = getVariantsResponse.stream().map(
                AccessionResponseDTO::getData).collect(Collectors.toSet());
        assertTrue(expectedVariants.stream().map(AccessionedDocument::getModel).map(modelToDto).allMatch(
                variantsReturnedByController::contains));
    }
}
