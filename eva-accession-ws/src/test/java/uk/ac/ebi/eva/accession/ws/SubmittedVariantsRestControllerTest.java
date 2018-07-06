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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.rest.AccessionResponseDTO;
import uk.ac.ebi.ampt2d.commons.accession.rest.BasicRestController;

import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.ws.rest.SubmittedVariantDTO;

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
    private BasicRestController basicRestController;

    @Autowired
    private TestRestTemplate testRestTemplate;

    private static final Long ACCESSION = 10000000001L;

    private static final String URL = "/v1/submitted-variants/";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = null;

    private static final Boolean MATCHES_ASSEMBLY = null;

    private static final Boolean ALLELES_MATCH = null;

    private static final Boolean VALIDATED = null;

    @Test
    public void testGetVariantsRestApi() throws AccessionCouldNotBeGeneratedException {
        List<AccessionResponseDTO> generatedAccessions = basicRestController.generateAccessions(
                getListOfVariantMessages());
        assertEquals(2, generatedAccessions.size());
        String accessions = generatedAccessions.stream().map(acc -> acc.getAccession().toString()).collect(
                Collectors.joining(","));
        String getVariantsUrl = URL + accessions;
        ResponseEntity<List> getVariantsResponse = testRestTemplate.getForEntity(getVariantsUrl, List.class);
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(2, getVariantsResponse.getBody().size());
    }

    public List<SubmittedVariantDTO> getListOfVariantMessages() {
        SubmittedVariantDTO variant1 = new SubmittedVariantDTO("ASMACC01", 1101, "PROJACC01", "CHROM1", 1234, "REF",
                                                               "ALT", CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                               MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariantDTO variant2 = new SubmittedVariantDTO("ASMACC02", 1102, "PROJACC02", "CHROM2", 1234, "REF",
                                                               "ALT", CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                               MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        return asList(variant1, variant2);
    }
}
