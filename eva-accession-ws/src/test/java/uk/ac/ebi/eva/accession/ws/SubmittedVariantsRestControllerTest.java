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
import org.springframework.data.mongodb.core.MongoTemplate;
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
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.HashAlreadyExistsException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.DbsnpSubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.DbsnpSubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.ws.rest.SubmittedVariantsRestController;

import java.util.Arrays;
import java.util.List;

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
    private DbsnpSubmittedVariantMonotonicAccessioningService dbsnpService;

    @Autowired
    private DbsnpSubmittedVariantInactiveService dbsnpInactiveService;

    @Autowired
    private SubmittedVariantsRestController controller;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    private List<AccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions;

    private SubmittedVariant variant1;

    private SubmittedVariant variant2;

    @Before
    public void setUp() throws AccessionCouldNotBeGeneratedException {
        repository.deleteAll();
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
        mongoTemplate.dropCollection(SubmittedVariantOperationEntity.class);

        Long CLUSTERED_VARIANT = null;
        variant1 = new SubmittedVariant("ASMACC01", 1101, "PROJACC01", "CHROM1", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        variant2 = new SubmittedVariant("ASMACC02", 1102, "PROJACC02", "CHROM2", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        generatedAccessions = service.getOrCreate(Arrays.asList(variant1, variant2));
    }

    @After
    public void tearDown() {
        repository.deleteAll();
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
        mongoTemplate.dropCollection(SubmittedVariantOperationEntity.class);
    }

    @Test
    public void testGetVariantsRestTemplate() {
        for (AccessionWrapper<ISubmittedVariant, String, Long> generatedAccession : generatedAccessions) {
            String getVariantsUrl = URL + generatedAccession.getAccession();

            ResponseEntity<List<
                    AccessionResponseDTO<
                            SubmittedVariant,
                            ISubmittedVariant,
                            String,
                            Long>>> getVariantsResponse =
                    testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new SubmittedVariantType());
            assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
            assertEquals(1, getVariantsResponse.getBody().size());
            assertDefaultFlags(getVariantsResponse.getBody());
            assertCreatedDateNotNull(getVariantsResponse.getBody());
        }
    }

    private static class SubmittedVariantType extends ParameterizedTypeReference<List<
            AccessionResponseDTO<
                    SubmittedVariant,
                    ISubmittedVariant,
                    String,
                    Long>>> {
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

            assertEquals(1, getVariantsResponse.size());
            assertCreatedDateNotNull(getVariantsResponse);
            assertDefaultFlags(getVariantsResponse);
        }
    }

    @Test
    public void testGetRedirectionForMergedVariants()
            throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException,
                   AccessionDeprecatedException {
        // given
        Long CLUSTERED_VARIANT = null;
        SubmittedVariant variant1 = new SubmittedVariant("ASMACC01", 2000, "PROJACC01", "CHROM1", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        SubmittedVariant variant2 = new SubmittedVariant("ASMACC02", 2000, "PROJACC02", "CHROM2", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getOrCreate(
                Arrays.asList(variant1, variant2));

        Long outdatedAccession = accessions.get(0).getAccession();
        Long currentAccession = accessions.get(1).getAccession();
        service.merge(outdatedAccession,
                      currentAccession,
                      "Just for testing the endpoint, let's pretend the variants are equivalent");

        // when
        String getVariantsUrl = URL + outdatedAccession;
        ResponseEntity<String> firstResponse = testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                                                         String.class);

        // then
        assertEquals(HttpStatus.MOVED_PERMANENTLY, firstResponse.getStatusCode());
        String redirectUrlIncludingHostAndPort = firstResponse.getHeaders().get(HttpHeaders.LOCATION).get(0);
        String redirectedUrl = redirectUrlIncludingHostAndPort.substring(redirectUrlIncludingHostAndPort.indexOf(URL));
        assertEquals(URL + currentAccession, redirectedUrl);

        // and then
        ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(redirectedUrl, HttpMethod.GET, null, new SubmittedVariantType());

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(1, getVariantsResponse.getBody().size());
        assertEquals(currentAccession, getVariantsResponse.getBody().get(0).getAccession());
        assertDefaultFlags(getVariantsResponse.getBody());
        assertCreatedDateNotNull(getVariantsResponse.getBody());
    }

    /**
     * If there is a variant with operations of several types (e.g. UPDATED and MERGED) the MERGED event should take
     * priority and the endpoint should return a redirection, **even if the MERGED event is not the last one**.
     *
     * Example dbsnp variant: ss825691104. It was declustered from rs796064771 and merged into ss825691103 at the same
     * time.
     */
    @Test
    public void testGetRedirectionForMergedAndUpdatedVariants()
            throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException, HashAlreadyExistsException {
        // given
        Long CLUSTERED_VARIANT = null;
        SubmittedVariant variant1 = new SubmittedVariant("ASMACC01", 2000, "PROJACC01", "CHROM1", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        Long outdatedAccession = 1L;
        SubmittedVariantEntity submittedVariantEntity1 = new SubmittedVariantEntity(outdatedAccession, "hash-100",
                                                                                    variant1, 1);
        Long currentAccession = 2L;
        SubmittedVariant variant2 = new SubmittedVariant("ASMACC02", 2000, "PROJACC02", "CHROM2", 1234, "REF", "ALT",
                                                         CLUSTERED_VARIANT);
        SubmittedVariantEntity submittedVariantEntity2 = new SubmittedVariantEntity(currentAccession, "hash-200",
                                                                                    variant2, 1);

        mongoTemplate.insert(Arrays.asList(submittedVariantEntity1, submittedVariantEntity2),
                             DbsnpSubmittedVariantEntity.class);

        dbsnpService.merge(outdatedAccession,
                           currentAccession,
                           "Just for testing the endpoint, let's pretend the variants are equivalent");

        SubmittedVariant updatedVariant = new SubmittedVariant(variant1);
        updatedVariant.setContig("contig_2");

        dbsnpInactiveService.update(new DbsnpSubmittedVariantEntity(outdatedAccession, "hash-300", updatedVariant, 1),
                                    "update a merged variant");

        // when
        String getVariantsUrl = URL + outdatedAccession;
        ResponseEntity<String> firstResponse = testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                                                         String.class);

        // then
        assertEquals(HttpStatus.MOVED_PERMANENTLY, firstResponse.getStatusCode());
        String redirectUrlIncludingHostAndPort = firstResponse.getHeaders().get(HttpHeaders.LOCATION).get(0);
        String redirectedUrl = redirectUrlIncludingHostAndPort.substring(redirectUrlIncludingHostAndPort.indexOf(URL));
        assertEquals(URL + currentAccession, redirectedUrl);

        // and then
        ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(redirectedUrl, HttpMethod.GET, null, new SubmittedVariantType());

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(1, getVariantsResponse.getBody().size());
        assertEquals(currentAccession, getVariantsResponse.getBody().get(0).getAccession());
        assertDefaultFlags(getVariantsResponse.getBody());
        assertCreatedDateNotNull(getVariantsResponse.getBody());
    }

    @Test
    public void testGetDeprecatedSubmittedVariant()
            throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException,
                   AccessionDeprecatedException {
        // given
        Long accession = generatedAccessions.get(0).getAccession();
        service.deprecate(accession, "deprecated for testing");
        String getVariantUrl = URL + accession;

        // when
        ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>> response =
                testRestTemplate.exchange(getVariantUrl, HttpMethod.GET, null, new SubmittedVariantType());

        // then
        assertEquals(HttpStatus.GONE, response.getStatusCode());
        assertEquals(1, response.getBody().size());
        assertEquals(variant1, response.getBody().get(0).getData());
        assertCreatedDateNotNull(response.getBody());
    }
}
