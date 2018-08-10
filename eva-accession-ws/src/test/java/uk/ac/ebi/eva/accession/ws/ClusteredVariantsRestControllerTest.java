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
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ClusteredVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantsRestControllerTest {

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository repository;

    @Autowired
    private BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController;

    @Autowired
    private TestRestTemplate testRestTemplate;

    private static final String URL = "/v1/clustered-variants/";

    @Test
    public void testGetVariantsRestApi() throws AccessionCouldNotBeGeneratedException {
        // No new dbSNP accessions can be generated, so the variants can only be stored directly using a repository
        // TODO
        List<DbsnpClusteredVariantEntity> variantsToSave = getListOfVariantMessages();
        repository.save(variantsToSave);

        String accessions = variantsToSave.stream().map(acc -> acc.getAccession().toString()).collect(Collectors.joining(","));
        String getVariantsUrl = URL + accessions;

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

    private void assertDefaultFlags(
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> body) {
        ClusteredVariant variant = body.get(0).getData();
        assertEquals(IClusteredVariant.DEFAULT_VALIDATED, variant.isValidated());
    }

    public List<DbsnpClusteredVariantEntity> getListOfVariantMessages() {
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 1101, "CHROM1", 1234, VariantType.SNV, false);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC02", 1102, "CHROM2", 1234, VariantType.MNV, false);

        DbsnpClusteredVariantSummaryFunction function = new DbsnpClusteredVariantSummaryFunction();
        DbsnpClusteredVariantEntity entity1 = new DbsnpClusteredVariantEntity(1L, function.apply(variant1), variant1);
        DbsnpClusteredVariantEntity entity2 = new DbsnpClusteredVariantEntity(2L, function.apply(variant2), variant2);

        return asList(entity1, entity2);
    }
}
