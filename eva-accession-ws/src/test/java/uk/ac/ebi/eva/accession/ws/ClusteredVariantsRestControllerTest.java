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
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.rest.ClusteredVariantsRestController;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantsRestControllerTest {

    private static final String URL = "/v1/clustered-variants/";

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_1 = 1L;

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_2 = 2L;

    private static final Long DBSNP_CLUSTERED_VARIANT_ACCESSION_3 = 3L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_1 = 11L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_2 = 12L;

    private static final Long EVA_SUBMITTED_VARIANT_ACCESSION_1 = 1001L;

    private static final Long EVA_SUBMITTED_VARIANT_ACCESSION_2 = 1002L;

    private static final int VERSION_1 = 1;

    private static final int VERSION_2 = 1;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository dbsnpSubmittedVariantRepository;

    @Autowired
    private SubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private ClusteredVariantsRestController controller;

    @Autowired
    private ClusteredVariantAccessioningService clusteredService;

    @Autowired
    private TestRestTemplate testRestTemplate;

    private Iterable<DbsnpClusteredVariantEntity> generatedAccessions;

    private DbsnpSubmittedVariantEntity submittedVariantEntity1;

    private DbsnpSubmittedVariantEntity submittedVariantEntity2;

    private SubmittedVariantEntity evaSubmittedVariantEntity3;

    private SubmittedVariantEntity evaSubmittedVariantEntity4;

    private DbsnpClusteredVariantEntity clusteredVariantEntity1;

    private DbsnpClusteredVariantEntity clusteredVariantEntity2;

    private DbsnpClusteredVariantEntity clusteredVariantEntity3;

    @Before
    public void setUp() {
        setupDbSnpClusteredVariants();
        setupDbsnpSubmittedVariants();
        setupEvaSubmittedVariants();
    }

    private void setupDbSnpClusteredVariants() {
        dbsnpRepository.deleteAll();
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 1101, "CHROM1", 1234, VariantType.SNV, false,
                                                         null);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC01", 1102, "CHROM1", 1234, VariantType.MNV, true, null);
        ClusteredVariant variant3 = new ClusteredVariant("ASMACC01", 1102, "CHROM1", 4567, VariantType.SNV, false,
                                                         null);

        ClusteredVariantSummaryFunction function = new ClusteredVariantSummaryFunction();
        clusteredVariantEntity1 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                                                                              function.apply(variant1), variant1);
        clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                                                                              function.apply(variant2), variant2);
        clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_3,
                                                                              function.apply(variant3), variant3);

        // No new dbSNP accessions can be generated, so the variants can only be stored directly using a repository
        // TODO When the support for new EVA accessions is implemented, this could be changed
        // In order to do so, replicate the structure of {@link SubmittedVariantsRestControllerTest}
        generatedAccessions = dbsnpRepository.save(Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity2,
                                                                 clusteredVariantEntity3));
    }

    private void setupDbsnpSubmittedVariants() {
        dbsnpSubmittedVariantRepository.deleteAll();
        // one variant has default flags, the other have no default values
        SubmittedVariant submittedVariant1 = new SubmittedVariant("ASMACC01", 1101, "PROJECT1", "CHROM1", 1234, "REF",
                                                                  "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_1);
        SubmittedVariant submittedVariant2 = new SubmittedVariant("ASMACC01", 1102, "PROJECT1", "CHROM1", 2345, "REF",
                                                                  "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                                                                  !ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                  !ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH,
                                                                  !ISubmittedVariant.DEFAULT_ALLELES_MATCH,
                                                                  !ISubmittedVariant.DEFAULT_VALIDATED, null);

        SubmittedVariantSummaryFunction submittedVariantSummaryFunction = new SubmittedVariantSummaryFunction();
        submittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(DBSNP_SUBMITTED_VARIANT_ACCESSION_1,
                                                submittedVariantSummaryFunction.apply(submittedVariant1),
                                                submittedVariant1, VERSION_1);
        submittedVariantEntity2 =
                new DbsnpSubmittedVariantEntity(DBSNP_SUBMITTED_VARIANT_ACCESSION_2,
                                                submittedVariantSummaryFunction.apply(submittedVariant2),
                                                submittedVariant2, VERSION_1);

        dbsnpSubmittedVariantRepository.save(Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));
    }

    private void setupEvaSubmittedVariants() {
        submittedVariantRepository.deleteAll();
        // one variant has no default flags, while the others have the default values
        SubmittedVariant submittedVariant3 = new SubmittedVariant("ASMACC01", 1102, "EVAPROJECT1", "CHROM1", 1234,
                                                                  "REF", "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_2);
        SubmittedVariant submittedVariant4 = new SubmittedVariant("ASMACC01", 1102, "EVAPROJECT1", "CHROM1", 4567,
                                                                  "REF", "ALT", DBSNP_CLUSTERED_VARIANT_ACCESSION_3,
                                                                  !ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                  !ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH,
                                                                  !ISubmittedVariant.DEFAULT_ALLELES_MATCH,
                                                                  !ISubmittedVariant.DEFAULT_VALIDATED, null);

        SubmittedVariantSummaryFunction submittedVariantSummaryFunction = new SubmittedVariantSummaryFunction();
        evaSubmittedVariantEntity3 =
                new SubmittedVariantEntity(EVA_SUBMITTED_VARIANT_ACCESSION_1,
                                           submittedVariantSummaryFunction.apply(submittedVariant3),
                                           submittedVariant3, VERSION_1);
        evaSubmittedVariantEntity4 =
                new SubmittedVariantEntity(EVA_SUBMITTED_VARIANT_ACCESSION_2,
                                           submittedVariantSummaryFunction.apply(submittedVariant4),
                                           submittedVariant4, VERSION_2);

        submittedVariantRepository.save(Arrays.asList(evaSubmittedVariantEntity3, evaSubmittedVariantEntity4));
    }

    @After
    public void tearDown() {
        dbsnpRepository.deleteAll();
        dbsnpSubmittedVariantRepository.deleteAll();
        submittedVariantRepository.deleteAll();
    }

    @Test
    public void testGetVariantsRestApi() {
        for (DbsnpClusteredVariantEntity generatedAccession : generatedAccessions) {
            String getVariantsUrl = URL + generatedAccession.getAccession();
            ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                    getVariantsResponse =
                    testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new ClusteredVariantType());
            assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> wsResponseBody =
                    getVariantsResponse.getBody();
            checkClusteredVariantsOutput(wsResponseBody, generatedAccession.getAccession());
        }
    }

    private static class ClusteredVariantType extends ParameterizedTypeReference<List<
            AccessionResponseDTO<
                    ClusteredVariant,
                    IClusteredVariant,
                    String,
                    Long>>> {
    }

    private void checkClusteredVariantsOutput(
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse,
            Long accession) {
        List<AccessionedDocument<IClusteredVariant, Long>> expectedVariants =
                Stream.of(clusteredVariantEntity1, clusteredVariantEntity2, clusteredVariantEntity3)
                .filter(v -> v.getAccession().equals(accession))
                .collect(Collectors.toList());
        assertVariantsAreContainedInControllerResponse(getVariantsResponse,
                                                       expectedVariants,
                                                       ClusteredVariant::new);
        assertClusteredVariantCreatedDateNotNull(getVariantsResponse);
    }

    private <DTO, MODEL> void assertVariantsAreContainedInControllerResponse(
            List<AccessionResponseDTO<DTO, MODEL, String, Long>> getVariantsResponse,
            List<AccessionedDocument<MODEL, Long>> expectedVariants,
            Function<MODEL, DTO> modelToDto) {
        // check the accessions returned by the service
        Set<Long> retrievedAccessions = getVariantsResponse.stream()
                                                           .map(AccessionResponseDTO::getAccession)
                                                           .collect(Collectors.toSet());

        assertTrue(expectedVariants.stream()
                                   .map(AccessionedDocument::getAccession)
                                   .allMatch(retrievedAccessions::contains));

        // check the objects returned by the service
        Set<DTO> variantsReturnedByController = getVariantsResponse.stream()
                                                                   .map(AccessionResponseDTO::getData)
                                                                   .collect(Collectors.toSet());

        assertTrue(expectedVariants.stream()
                                   .map(AccessionedDocument::getModel)
                                   .map(modelToDto)
                                   .allMatch(variantsReturnedByController::contains));
    }

    private void assertClusteredVariantCreatedDateNotNull(
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> body) {
        body.forEach(accessionResponseDTO -> assertNotNull(accessionResponseDTO.getData().getCreatedDate()));
    }

    @Test
    public void testGetSubmittedVariantsRestApi() {
        for (DbsnpClusteredVariantEntity generatedAccession : generatedAccessions) {
            String getVariantsUrl = URL + generatedAccession.getAccession() + "/submitted";
            ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>>
                    getVariantsResponse =
                    testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new SubmittedVariantType());
            assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> wsResponseBody =
                    getVariantsResponse.getBody();
            checkSubmittedVariantsOutput(wsResponseBody, generatedAccession.getAccession());
        }
    }

    private static class SubmittedVariantType extends ParameterizedTypeReference<List<
            AccessionResponseDTO<
                    SubmittedVariant,
                    ISubmittedVariant,
                    String,
                    Long>>> {
    }

    private void checkSubmittedVariantsOutput(
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getSubmittedVariantsReponse,
            Long accession) {
        List<AccessionedDocument<ISubmittedVariant, Long>> expectedVariants =
                Stream.of(submittedVariantEntity1, submittedVariantEntity2, evaSubmittedVariantEntity3,
                          evaSubmittedVariantEntity4)
                      .filter(v -> v.getAccession().equals(accession))
                      .collect(Collectors.toList());

        assertVariantsAreContainedInControllerResponse(getSubmittedVariantsReponse,
                                                       expectedVariants,
                                                       SubmittedVariant::new);
        assertSubmittedVariantCreatedDateNotNull(getSubmittedVariantsReponse);
    }

    private void assertSubmittedVariantCreatedDateNotNull(
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> body) {
        body.forEach(accessionResponseDTO -> assertNotNull(accessionResponseDTO.getData().getCreatedDate()));
    }

    @Test
    public void testGetVariantsController()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        for (DbsnpClusteredVariantEntity generatedAccession : generatedAccessions) {
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse =
                    controller.get(generatedAccession.getAccession());
            checkClusteredVariantsOutput(getVariantsResponse, generatedAccession.getAccession());
        }
    }

    @Test
    public void testGetSubmittedVariantsByClusteredVariantIds()
            throws AccessionDoesNotExistException, AccessionDeprecatedException, AccessionMergedException {
        getAndCheckSubmittedVariantsByClusteredVariantIds(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                Collections.singletonList(submittedVariantEntity1));
        getAndCheckSubmittedVariantsByClusteredVariantIds(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                Arrays.asList(submittedVariantEntity2, evaSubmittedVariantEntity3));
        getAndCheckSubmittedVariantsByClusteredVariantIds(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_3,
                Collections.singletonList(evaSubmittedVariantEntity4));
    }

    private void getAndCheckSubmittedVariantsByClusteredVariantIds(Long clusteredVariantIds,
                                                                   List<AccessionedDocument<ISubmittedVariant, Long>>
                                                                           expectedSubmittedVariants)
            throws AccessionDoesNotExistException, AccessionDeprecatedException, AccessionMergedException {
        List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantsResponse =
                controller.getSubmittedVariants(clusteredVariantIds);
        assertVariantsAreContainedInControllerResponse(getVariantsResponse,
                                                       expectedSubmittedVariants,
                                                       SubmittedVariant::new);
    }

    @Test
    public void testGetRedirectionForMergedVariants()
            throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException,
                   AccessionDeprecatedException {
        // given
        clusteredService.merge(DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                               DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                               "Just for testing the endpoint, let's pretend the variants are equivalent");

        // when
        String getVariantsUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_1;
        ResponseEntity<String> firstResponse = testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                                                         String.class);

        // then
        assertEquals(HttpStatus.MOVED_PERMANENTLY, firstResponse.getStatusCode());
        String redirectUrlIncludingHostAndPort = firstResponse.getHeaders().get(HttpHeaders.LOCATION).get(0);
        String redirectedUrl = redirectUrlIncludingHostAndPort.substring(redirectUrlIncludingHostAndPort.indexOf(URL));
        assertEquals(URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_2, redirectedUrl);

        // and then
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(redirectedUrl, HttpMethod.GET, null, new ClusteredVariantType());

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(1, getVariantsResponse.getBody().size());
        assertEquals(new Long(DBSNP_CLUSTERED_VARIANT_ACCESSION_2), getVariantsResponse.getBody().get(0).getAccession());
        assertClusteredVariantCreatedDateNotNull(getVariantsResponse.getBody());
    }

    @Test
    public void testGetRedirectionForSubmittedByMergedClustered()
            throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException,
                   AccessionDeprecatedException {
        // given
        clusteredService.merge(DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                               DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                               "Just for testing the endpoint, let's pretend the variants are equivalent");

        // when
        String getVariantsUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_1 + "/submitted";
        ResponseEntity<String> firstResponse = testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                                                         String.class);

        // then
        assertEquals(HttpStatus.MOVED_PERMANENTLY, firstResponse.getStatusCode());
        String redirectUrlIncludingHostAndPort = firstResponse.getHeaders().get(HttpHeaders.LOCATION).get(0);
        String redirectedUrl = redirectUrlIncludingHostAndPort.substring(redirectUrlIncludingHostAndPort.indexOf(URL));
        assertEquals(URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_2 + "/submitted", redirectedUrl);

        // and then
        ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(redirectedUrl, HttpMethod.GET, null, new SubmittedVariantType());

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(2, getVariantsResponse.getBody().size());
        for (AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long> bodyEntry :
                getVariantsResponse.getBody()) {
            assertEquals(new Long(DBSNP_CLUSTERED_VARIANT_ACCESSION_2),
                         bodyEntry.getData().getClusteredVariantAccession());
        }

        assertSubmittedVariantCreatedDateNotNull(getVariantsResponse.getBody());
    }
}
