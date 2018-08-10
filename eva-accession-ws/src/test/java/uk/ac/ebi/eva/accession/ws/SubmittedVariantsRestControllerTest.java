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
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class SubmittedVariantsRestControllerTest {

    @Autowired
    private BasicRestController<SubmittedVariant, ISubmittedVariant, String, Long> basicRestController;

    @Autowired
    private TestRestTemplate testRestTemplate;

    private static final String URL = "/v1/submitted-variants/";

    @Test
    public void testGetVariantsRestApi() throws AccessionCouldNotBeGeneratedException {
        List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> generatedAccessions =
                basicRestController.generateAccessions(getListOfVariantMessages());
        assertEquals(2, generatedAccessions.size());
        String accessions = generatedAccessions.stream().map(acc -> acc.getAccession().toString()).collect(
                Collectors.joining(","));
        String getVariantsUrl = URL + accessions;
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
    }

    private void assertDefaultFlags(
            List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> body) {
        SubmittedVariant variant = body.get(0).getData();
        assertEquals(ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE, variant.isSupportedByEvidence());
        assertEquals(ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH, variant.isAssemblyMatch());
        assertEquals(ISubmittedVariant.DEFAULT_ALLELES_MATCH, variant.isAllelesMatch());
        assertEquals(ISubmittedVariant.DEFAULT_VALIDATED, variant.isValidated());
    }

    public List<SubmittedVariant> getListOfVariantMessages() {
        Long CLUSTERED_VARIANT = null;
        SubmittedVariant variant1 = new SubmittedVariant("ASMACC01", 1101, "PROJACC01", "CHROM1", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        SubmittedVariant variant2 = new SubmittedVariant("ASMACC02", 1102, "PROJACC02", "CHROM2", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        return asList(variant1, variant2);
    }
}
