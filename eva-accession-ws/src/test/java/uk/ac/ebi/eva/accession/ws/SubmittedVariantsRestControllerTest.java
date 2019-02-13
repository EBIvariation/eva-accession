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
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.ws.rest.SubmittedVariantsRestController;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class SubmittedVariantsRestControllerTest {

    private static final String URL = "/v1/submitted-variants/";

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Autowired
    private SubmittedVariantsRestController controller;

    @Autowired
    private TestRestTemplate testRestTemplate;

    private List<AccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions;

    @Before
    public void setUp() throws AccessionCouldNotBeGeneratedException {
        repository.deleteAll();

        Long CLUSTERED_VARIANT = null;
        SubmittedVariant variant1 = new SubmittedVariant("ASMACC01", 1101, "PROJACC01", "CHROM1", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        SubmittedVariant variant2 = new SubmittedVariant("ASMACC02", 1102, "PROJACC02", "CHROM2", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        generatedAccessions = service.getOrCreate(Arrays.asList(variant1, variant2));
    }

    @After
    public void tearDown() {
        repository.deleteAll();
    }

    @Test
    public void testGetVariantsRestTemplate() {
        for (AccessionWrapper<ISubmittedVariant, String, Long> generatedAccession : generatedAccessions) {
            String getVariantsUrl = URL + generatedAccession.getAccession();

            ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>>
                    getVariantsResponse =
                    testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                              new ParameterizedTypeReference<
                                                      List<
                                                              AccessionResponseDTO<
                                                                      SubmittedVariant,
                                                                      ISubmittedVariant,
                                                                      String,
                                                                      Long>>>() {
                                              });
            assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
            assertEquals(2, getVariantsResponse.getBody().size());
            assertDefaultFlags(getVariantsResponse.getBody());
            assertCreatedDateNotNull(getVariantsResponse.getBody());
        }
    }

    private void assertDefaultFlags(
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> body) {
        for (AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long> dto : body) {
            SubmittedVariant variant = dto.getData();
            assertEquals(DEFAULT_SUPPORTED_BY_EVIDENCE, variant.isSupportedByEvidence());
            assertEquals(DEFAULT_ASSEMBLY_MATCH, variant.isAssemblyMatch());
            assertEquals(DEFAULT_ALLELES_MATCH, variant.isAllelesMatch());
            assertEquals(DEFAULT_VALIDATED, variant.isValidated());
        }
    }

    private void assertCreatedDateNotNull(
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> body) {
        for (AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long> dto : body) {
            SubmittedVariant variant = dto.getData();
            assertNotNull(variant.getCreatedDate());
        }
    }

    @Test
    public void testGetVariantsController()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        for (AccessionWrapper<ISubmittedVariant, String, Long> generatedAccession : generatedAccessions) {
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantsResponse =
                    controller.get(generatedAccession.getAccession());

            assertEquals(2, getVariantsResponse.size());
            assertCreatedDateNotNull(getVariantsResponse);
            assertDefaultFlags(getVariantsResponse);
        }
    }
}
