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

import com.mongodb.BasicDBObject;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.server.ResponseStatusException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantOperationService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.rest.ClusteredVariantsRestController;
import uk.ac.ebi.eva.accession.ws.service.ClusteredVariantsBeaconService;
import uk.ac.ebi.eva.accession.ws.test.NoContigTranslationArgumentMatcher;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleRequest;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleResponse;
import uk.ac.ebi.eva.commons.beacon.models.BeaconDatasetAlleleResponse;
import uk.ac.ebi.eva.commons.beacon.models.KeyValuePair;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import javax.servlet.http.HttpServletResponse;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import({ClusteredVariantAccessioningConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-ws-test.properties")
public class ClusteredVariantsRestControllerTest {

    private static final String URL = "/v1/clustered-variants/";

    private static final String ENA_CONTIG_SUFFIX = "_ENA";

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_1 = 1L;

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_2 = 2L;

    private static final Long DBSNP_CLUSTERED_VARIANT_ACCESSION_3 = 3L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_1 = 11L;

    private static final long DBSNP_SUBMITTED_VARIANT_ACCESSION_2 = 12L;

    private static final Long EVA_SUBMITTED_VARIANT_ACCESSION_1 = 1001L;

    private static final Long EVA_SUBMITTED_VARIANT_ACCESSION_2 = 1002L;

    private static final int VERSION_1 = 1;

    private static final int VERSION_2 = 1;

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_1 = 1118L;

    private static final long DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_2 = 1475292486L;

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private static final String INACTIVE_OBJECTS_HASHED_MESSAGE = "inactiveObjects.hashedMessage";

    @Autowired
    private ClusteredVariantAccessioningRepository clusteredVariantAccessioningRepository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private HumanDbsnpClusteredVariantAccessioningService humanDbsnpClusteredVariantAccessioningService;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository dbsnpSubmittedVariantRepository;

    @Autowired
    private SubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private ClusteredVariantsRestController controller;

    @Autowired
    @Qualifier("nonhumanActiveService")
    private ClusteredVariantAccessioningService clusteredService;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("humanMongoTemplate")
    private MongoTemplate humanMongoTemplate;

    @Autowired
    @Qualifier("dbsnpClusteredService")
    private DbsnpClusteredVariantMonotonicAccessioningService dbsnpService;

    private ClusteredVariantsRestController mockController;

    private Iterable<DbsnpClusteredVariantEntity> generatedAccessions;

    private DbsnpSubmittedVariantEntity submittedVariantEntity1;

    private DbsnpSubmittedVariantEntity submittedVariantEntity2;

    private SubmittedVariantEntity evaSubmittedVariantEntity3;

    private SubmittedVariantEntity evaSubmittedVariantEntity4;

    private DbsnpClusteredVariantEntity clusteredVariantEntity1;

    private DbsnpClusteredVariantEntity clusteredVariantEntity2;

    private DbsnpClusteredVariantEntity clusteredVariantEntity3;

    private DbsnpClusteredVariantEntity clusteredHumanVariantEntity1;

    private DbsnpClusteredVariantEntity clusteredHumanVariantEntity2;

    private DbsnpClusteredVariantEntity clusteredHumanVariantEntity3;

    @Mock
    private ClusteredVariantOperationService clusteredVariantOperationService;

    @Mock
    private SubmittedVariantAccessioningService mockService;

    @Mock
    private HumanDbsnpClusteredVariantAccessioningService mockHumanService;

    @MockBean
    private ContigAliasService contigAliasService;

    @Before
    public void setUp() {
        dbsnpRepository.deleteAll();
        dbsnpSubmittedVariantRepository.deleteAll();
        submittedVariantRepository.deleteAll();
        mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY).deleteMany(new BasicDBObject());
        mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY).deleteMany(new BasicDBObject());
        humanMongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY).deleteMany(new BasicDBObject());
        humanMongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY).deleteMany(new BasicDBObject());
        setupDbSnpClusteredVariants();
        setupDbsnpSubmittedVariants();
        setupEvaSubmittedVariants();
        setupDbSnpClusteredHumanVariants();
        setupDbSnpClusteredHumanOperations();

        ClusteredVariantsBeaconService mockBeaconService = Mockito.spy(
                new ClusteredVariantsBeaconService(clusteredService, mockHumanService, mockService));
        Mockito.doThrow(new RuntimeException("Some unexpected error")).when(mockBeaconService)
               .queryBeaconClusteredVariant("GCA_ERROR", "CHROM1", 123, VariantType.SNV, ContigNamingConvention.ENA_SEQUENCE_NAME, false);
        Mockito.doThrow(new RuntimeException("Some unexpected error")).when(mockHumanService)
               .getByIdFields("GCA_ERROR", "CHROM1", 123, VariantType.SNV,  ContigNamingConvention.INSDC);
        mockController = new ClusteredVariantsRestController(mockService, mockBeaconService, mockHumanService,
                clusteredService, clusteredVariantOperationService
        );
    }

    private void setupDbSnpClusteredVariants() {
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 1101, "CHROM1", 1234, VariantType.SNV, false,
                                                         null);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC01", 1102, "CHROM1", 1234, VariantType.MNV, true, null);
        ClusteredVariant variant3 = new ClusteredVariant("ASMACC01", 1102, "CHROM1", 4567, VariantType.SNV, false,
                                                         null);

        Function<IClusteredVariant, String> function =
                new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

        clusteredVariantEntity1 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                                                                  function.apply(variant1), variant1);
        clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_2,
                                                                  function.apply(variant2), variant2);
        clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(DBSNP_CLUSTERED_VARIANT_ACCESSION_3,
                                                                  function.apply(variant3), variant3);

        // No new dbSNP accessions can be generated, so the variants can only be stored directly using a repository
        // TODO When the support for new EVA accessions is implemented, this could be changed
        // In order to do so, replicate the structure of {@link SubmittedVariantsRestControllerTest}
        generatedAccessions = dbsnpRepository.saveAll(Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity2,
                                                                    clusteredVariantEntity3));

        setUpContigAliasMock();
    }

    private void setUpContigAliasMock() {
        NoContigTranslationArgumentMatcher contigMatcher = new NoContigTranslationArgumentMatcher();

        when(contigAliasService.translateContigToInsdc(anyString(), anyString(), argThat(contigMatcher)))
                .thenCallRealMethod();
        when(contigAliasService.translateContigToInsdc(anyString(), anyString(), eq(ContigNamingConvention.ENA_SEQUENCE_NAME)))
                .then(invocation -> {
                    String contigName = invocation.getArgument(0);
                    if (contigName.endsWith(ENA_CONTIG_SUFFIX)) {
                        return contigName.substring(0, contigName.length() - ENA_CONTIG_SUFFIX.length());
                    }
                    throw new NoSuchElementException("Tried to translate non-ENA contig using ENA naming convention");
                });
        when(contigAliasService.translateContigToInsdc(anyString(), anyString(), eq(ContigNamingConvention.UCSC)))
                .thenThrow(NoSuchElementException.class);

        when(contigAliasService.translateContigFromInsdc(anyString(), argThat(contigMatcher)))
                .thenCallRealMethod();
        when(contigAliasService.translateContigFromInsdc(any(), eq(ContigNamingConvention.ENA_SEQUENCE_NAME)))
                .then(invocation -> {
                    String contigName = invocation.getArgument(0);
                    if (!contigName.endsWith(ENA_CONTIG_SUFFIX)) {
                        return contigName + ENA_CONTIG_SUFFIX;
                    }
                    throw new NoSuchElementException("Tried to translate ENA contig using INSDC naming convention");
                });

        when(contigAliasService.createClusteredVariantAccessionWrapperWithNewContig(any(), anyString()))
                .thenCallRealMethod();

        when(contigAliasService.createSubmittedVariantAccessionWrapperWithNewContig(any(), anyString()))
                .thenCallRealMethod();

        when(contigAliasService.getClusteredVariantsWithTranslatedContig(any(), any()))
                .thenCallRealMethod();

        when(contigAliasService.getSubmittedVariantsWithTranslatedContig(any(), any()))
                .thenCallRealMethod();
    }

    private void setupDbSnpClusteredHumanVariants() {
        ClusteredVariant variant1 = new ClusteredVariant("GCA_000001405.27", 9606, "CM000684.2", 45565333L,
                                                         VariantType.SNV, false,
                                                         LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

        ClusteredVariant variant2 = new ClusteredVariant("GCA_000001405.27", 9606, "CM000663.2", 1234L,
                                                         VariantType.SNV, false,
                                                         LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

        Function<IClusteredVariant, String> function =
                new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

        clusteredHumanVariantEntity1 = new DbsnpClusteredVariantEntity(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_1, function.apply(variant1), variant1);
        clusteredHumanVariantEntity2 = new DbsnpClusteredVariantEntity(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_1, function.apply(variant2), variant2);
        humanMongoTemplate.insert(Arrays.asList(clusteredHumanVariantEntity1, clusteredHumanVariantEntity2),
                DbsnpClusteredVariantEntity.class);
    }

    private void setupDbSnpClusteredHumanOperations() {
        ClusteredVariant variant1 = new ClusteredVariant("GCA_000001405.27", 9606, "CM000663.2", 72348112L,
                VariantType.INDEL, false, LocalDateTime.of(2017, Month.NOVEMBER, 11, 9, 55, 0));

        Function<IClusteredVariant, String> function =
                new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

        clusteredHumanVariantEntity3 = new DbsnpClusteredVariantEntity(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_2, function.apply(variant1), variant1);
        DbsnpClusteredVariantInactiveEntity inactiveEntity = new DbsnpClusteredVariantInactiveEntity(
                clusteredHumanVariantEntity3);
        DbsnpClusteredVariantOperationEntity clusteredVariantOperationEntity1 =
                new DbsnpClusteredVariantOperationEntity();
        clusteredVariantOperationEntity1.fill(EventType.MERGED, DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_2, 777512306L,
                "Identical clustered variant received multiple RS identifiers",
                Collections.singletonList(inactiveEntity));

        humanMongoTemplate.insert(Collections.singleton(clusteredVariantOperationEntity1),
                DbsnpClusteredVariantOperationEntity.class);
    }

    private void setupDbsnpSubmittedVariants() {
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

        dbsnpSubmittedVariantRepository.saveAll(Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));
    }

    private void setupEvaSubmittedVariants() {
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

        submittedVariantRepository.saveAll(Arrays.asList(evaSubmittedVariantEntity3, evaSubmittedVariantEntity4));
    }

    @After
    public void tearDown() {
        dbsnpRepository.deleteAll();
        dbsnpSubmittedVariantRepository.deleteAll();
        submittedVariantRepository.deleteAll();
        mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY).deleteMany(new BasicDBObject());
        mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY).deleteMany(new BasicDBObject());
        humanMongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY).deleteMany(new BasicDBObject());
        humanMongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY).deleteMany(new BasicDBObject());
    }

    @Test
    public void checkIndexInactiveObjectHashedMessageOnlyInHumanDB() {
        assertFalse(isIndexInCollection(mongoTemplate, DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY,
                                        INACTIVE_OBJECTS_HASHED_MESSAGE));

        assertTrue(isIndexInCollection(humanMongoTemplate, DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY,
                                       INACTIVE_OBJECTS_HASHED_MESSAGE));
    }

    private boolean isIndexInCollection(MongoTemplate template, String collection, String indexName) {
        List<String> indexNames = new ArrayList<>();
        template.getCollection(collection).listIndexes().forEach(
                (Consumer<Document>) d -> indexNames.add(d.get("name").toString()));
        List<String> matchingIndexes = indexNames.stream().filter(name -> name.contains(indexName)).collect(
                Collectors.toList());
        return !matchingIndexes.isEmpty();
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

    @Test
    public void testGetHumanVariantsRestApi() {
        String getVariantsUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_1;
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new ClusteredVariantType());
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> wsResponseBody =
                getVariantsResponse.getBody();
        assertVariantsAreContainedInControllerResponse(wsResponseBody,
                Collections.singletonList(clusteredHumanVariantEntity1), ClusteredVariant::new);
    }

    @Test
    public void testGetHumanVariantsInOperationsRestApi() {
        String getVariantsUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_HUMAN_2;
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                getVariantsResponse =
                testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null, new ClusteredVariantType());
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> wsResponseBody =
                getVariantsResponse.getBody();
        assertVariantsAreContainedInControllerResponse(wsResponseBody,
                Collections.singletonList(clusteredHumanVariantEntity3), ClusteredVariant::new);
    }

    @Test
    public void testGetVariantsHumanAndNonHuman() {
        String getVariantsUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_1;
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                getVariantsResponse = testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                new ClusteredVariantType());
        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> wsResponseBody =
                getVariantsResponse.getBody();
        assertVariantsAreContainedInControllerResponse(wsResponseBody, Arrays.asList(clusteredVariantEntity1,
                clusteredHumanVariantEntity2), ClusteredVariant::new);
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

    private void checkClusteredVariantsUseEnaSequenceName(
            List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getVariantsResponse) {
        for (AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long> dto : getVariantsResponse) {
            ClusteredVariant variant = dto.getData();
            assertTrue(variant.getContig().endsWith(ENA_CONTIG_SUFFIX));
        }
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
            throws AccessionMergedException, AccessionDoesNotExistException {
        for (DbsnpClusteredVariantEntity generatedAccession : generatedAccessions) {
            ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                    getVariantsResponse = controller.get(generatedAccession.getAccession(), ContigNamingConvention.INSDC);
            checkClusteredVariantsOutput(getVariantsResponse.getBody(), generatedAccession.getAccession());
        }
    }

    @Test
    public void testGetVariantsController_withContigTranslation() throws AccessionDoesNotExistException,
            AccessionMergedException {
        for (DbsnpClusteredVariantEntity generatedAccession : generatedAccessions) {
            ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>>
                    getVariantsResponse = controller.get(generatedAccession.getAccession(), ContigNamingConvention.ENA_SEQUENCE_NAME);
            checkClusteredVariantsUseEnaSequenceName(getVariantsResponse.getBody());
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

    @Test
    public void testGetSubmittedVariantsByClusteredVariantIds_withContigTranslation()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantsResponse =
                controller.getSubmittedVariants(DBSNP_CLUSTERED_VARIANT_ACCESSION_1,
                                                ContigNamingConvention.ENA_SEQUENCE_NAME);
        DbsnpSubmittedVariantEntity expectedSubmittedVariant = getDbsnpSubmittedVariantEntityWithEnaContigName(
                submittedVariantEntity1);
        assertVariantsAreContainedInControllerResponse(getVariantsResponse,
                                                       Collections.singletonList(expectedSubmittedVariant),
                                                       SubmittedVariant::new);
    }

    private DbsnpSubmittedVariantEntity getDbsnpSubmittedVariantEntityWithEnaContigName(
            DbsnpSubmittedVariantEntity submittedVariantEntity) {
        return new DbsnpSubmittedVariantEntity(
                submittedVariantEntity.getAccession(),
                submittedVariantEntity.getHashedMessage(),
                submittedVariantEntity.getReferenceSequenceAccession(),
                submittedVariantEntity.getTaxonomyAccession(),
                submittedVariantEntity.getProjectAccession(),
                submittedVariantEntity.getContig() + ENA_CONTIG_SUFFIX,
                submittedVariantEntity.getStart(),
                submittedVariantEntity.getReferenceAllele(),
                submittedVariantEntity.getAlternateAllele(),
                submittedVariantEntity.getClusteredVariantAccession(),
                submittedVariantEntity.isSupportedByEvidence(),
                submittedVariantEntity.isAssemblyMatch(),
                submittedVariantEntity.isAllelesMatch(),
                submittedVariantEntity.isValidated(),
                submittedVariantEntity.getVersion()
        );
    }

    private void getAndCheckSubmittedVariantsByClusteredVariantIds(Long clusteredVariantIds,
                                                                   List<AccessionedDocument<ISubmittedVariant, Long>>
                                                                           expectedSubmittedVariants)
            throws AccessionDoesNotExistException, AccessionDeprecatedException, AccessionMergedException {
        List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantsResponse =
                controller.getSubmittedVariants(clusteredVariantIds, ContigNamingConvention.INSDC);
        assertVariantsAreContainedInControllerResponse(getVariantsResponse,
                                                       expectedSubmittedVariants,
                                                       SubmittedVariant::new);
    }

    @Test
    public void testGetRedirectionForMergedVariants()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
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
        assertEquals(new Long(DBSNP_CLUSTERED_VARIANT_ACCESSION_2),
                     getVariantsResponse.getBody().get(0).getAccession());
        assertClusteredVariantCreatedDateNotNull(getVariantsResponse.getBody());
    }

    @Test
    public void testGetRedirectionForSubmittedVariantByMergedClusteredVariant()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
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

    /**
     * There could be an RS merged into several other active RSs. Not entirely correct, but it was decided to return
     * only one of those merges as redirection, doesn't matter which one.
     */
    @Test
    public void testGetVariantsMergedSeveralTimes()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        // given
        Long outdatedAccession = 1L;
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 2000, "CHROM1", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = new DbsnpClusteredVariantEntity(outdatedAccession,
                                                                                              "hash-100", variant1, 1);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC02", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(outdatedAccession,
                                                                                              "hash-200", variant2, 1);

        Long currentAccession = 2L;
        ClusteredVariant variant4 = new ClusteredVariant("ASMACC03", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = new DbsnpClusteredVariantEntity(currentAccession,
                                                                                              "hash-400", variant4, 1);
        Long anotherCurrentAccession = 3L;
        ClusteredVariant variant5 = new ClusteredVariant("ASMACC04", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity5 = new DbsnpClusteredVariantEntity(anotherCurrentAccession,
                                                                                              "hash-500", variant5, 1);

        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity4, clusteredVariantEntity5),
                             DbsnpClusteredVariantEntity.class);
        dbsnpService.merge(outdatedAccession, currentAccession,
                           "Just for testing the endpoint, let's pretend the variants are equivalent");

        mongoTemplate.insert(Arrays.asList(clusteredVariantEntity2), DbsnpClusteredVariantEntity.class);
        dbsnpService.merge(outdatedAccession, anotherCurrentAccession,
                           "Second merge. This can totally happen importing from dbSNP. See rs106458077");

        // when
        String getVariantsUrl = URL + outdatedAccession;
        ResponseEntity<String> firstResponse = testRestTemplate.exchange(getVariantsUrl, HttpMethod.GET, null,
                                                                         String.class);

        // then
        assertEquals(HttpStatus.MOVED_PERMANENTLY, firstResponse.getStatusCode());
        String redirectUrlIncludingHostAndPort = firstResponse.getHeaders().get(HttpHeaders.LOCATION).get(0);
        String redirectedUrl = redirectUrlIncludingHostAndPort.substring(redirectUrlIncludingHostAndPort.indexOf(URL));
        assertTrue((URL + currentAccession).equals(redirectedUrl)
                           || (URL + anotherCurrentAccession).equals(redirectedUrl));

        // and then
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse = testRestTemplate
                .exchange(redirectedUrl, HttpMethod.GET, null, new ClusteredVariantType());

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(1, getVariantsResponse.getBody().size());
        assertTrue(Arrays.asList(currentAccession, anotherCurrentAccession)
                         .contains(getVariantsResponse.getBody().get(0).getAccession()));
        assertClusteredVariantCreatedDateNotNull(getVariantsResponse.getBody());
    }

    /**
     * There could be an RS merged into several other active RSs, and deprecated at the same time. This test checks that
     * the deprecation event is given priority over the other events.
     */
    @Test
    public void testGetMergedAndDeprecatedVariants()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        // given
        Long outdatedAccession = 1L;
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 2000, "CHROM1", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = new DbsnpClusteredVariantEntity(outdatedAccession,
                                                                                              "hash-100", variant1, 1);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC02", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(outdatedAccession,
                                                                                              "hash-200", variant2, 1);
        ClusteredVariant variant3 = new ClusteredVariant("ASMACC02", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(outdatedAccession,
                                                                                              "hash-300", variant3, 1);

        Long currentAccession = 2L;
        ClusteredVariant variant4 = new ClusteredVariant("ASMACC02", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = new DbsnpClusteredVariantEntity(currentAccession,
                                                                                              "hash-400", variant4, 1);
        Long anotherCurrentAccession = 3L;
        ClusteredVariant variant5 = new ClusteredVariant("ASMACC01", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity5 = new DbsnpClusteredVariantEntity(anotherCurrentAccession,
                                                                                              "hash-500", variant5, 1);

        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity4, clusteredVariantEntity5),
                             DbsnpClusteredVariantEntity.class);
        dbsnpService.merge(outdatedAccession, currentAccession,
                           "Just for testing the endpoint, let's pretend the variants are equivalent");

        mongoTemplate.insert(Arrays.asList(clusteredVariantEntity2), DbsnpClusteredVariantEntity.class);
        dbsnpService.merge(outdatedAccession, anotherCurrentAccession,
                           "Second merge. This can totally happen importing from dbSNP. See rs106458077");

        mongoTemplate.insert(Arrays.asList(clusteredVariantEntity3), DbsnpClusteredVariantEntity.class);
        dbsnpService.deprecate(outdatedAccession, "And then deprecated it.");

        // when
        String getVariantUrl = URL + outdatedAccession;
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> response =
                testRestTemplate.exchange(getVariantUrl, HttpMethod.GET, null, new ClusteredVariantType());

        // then
        assertEquals(HttpStatus.GONE, response.getStatusCode());
        assertEquals(1, response.getBody().size());
        assertEquals(clusteredVariantEntity3.getModel(), response.getBody().get(0).getData());
        assertClusteredVariantCreatedDateNotNull(response.getBody());
    }

    @Test
    public void testGetDeprecatedDbsnpClusteredVariant()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        // given
        clusteredService.deprecate(DBSNP_CLUSTERED_VARIANT_ACCESSION_1, "deprecated for testing");
        String getVariantUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_1;

        // when
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> response =
                testRestTemplate.exchange(getVariantUrl, HttpMethod.GET, null, new ClusteredVariantType());

        // then
        assertEquals(HttpStatus.GONE, response.getStatusCode());
        assertEquals(1, response.getBody().size());
        assertEquals(clusteredVariantEntity1.getModel(), response.getBody().get(0).getData());
        assertClusteredVariantCreatedDateNotNull(response.getBody());
    }

    /**
     * Note that the design is to return any of those documents that have a given accession as deprecated
     */
    @Test
    public void testGetSeveralDeprecatedDbsnpClusteredVariants()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        // given
        clusteredService.deprecate(DBSNP_CLUSTERED_VARIANT_ACCESSION_1, "deprecated for testing");
        ClusteredVariant modifiedVariant = new ClusteredVariant(clusteredVariantEntity1);
        modifiedVariant.setTaxonomyAccession(modifiedVariant.getTaxonomyAccession() + 1);
        DbsnpClusteredVariantEntity clusteredVariantEntityCopy = new DbsnpClusteredVariantEntity(
                DBSNP_CLUSTERED_VARIANT_ACCESSION_1, clusteredVariantEntity1.getHashedMessage(), modifiedVariant);
        dbsnpRepository.saveAll(Arrays.asList(clusteredVariantEntityCopy));
        clusteredService.deprecate(DBSNP_CLUSTERED_VARIANT_ACCESSION_1, "deprecated again");

        String getVariantUrl = URL + DBSNP_CLUSTERED_VARIANT_ACCESSION_1;

        // when
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> response =
                testRestTemplate.exchange(getVariantUrl, HttpMethod.GET, null, new ClusteredVariantType());

        // then
        assertEquals(HttpStatus.GONE, response.getStatusCode());
        assertEquals(1, response.getBody().size());

        ClusteredVariant data = response.getBody().get(0).getData();
        assertTrue(modifiedVariant.equals(data) || clusteredVariantEntity1.equals(data));
        assertClusteredVariantCreatedDateNotNull(response.getBody());
    }

    @Test
    public void testGetDeprecatedEvaClusteredVariant()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        // given
        Long deprecatedAccession = 1L;
        ClusteredVariant variant1 = new ClusteredVariant("ASMACC01", 2000, "CHROM1", 1234, VariantType.SNV, false,
                                                         null);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = new DbsnpClusteredVariantEntity(deprecatedAccession,
                                                                                              "hash-100", variant1, 1);
        ClusteredVariant variant2 = new ClusteredVariant("ASMACC02", 2000, "CHROM2", 1234, VariantType.SNV, false,
                                                         null);
        Long otherAccession = 2L;
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(otherAccession,
                                                                                              "hash-200", variant2, 1);

        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(clusteredVariantEntity1, clusteredVariantEntity2),
                             DbsnpClusteredVariantEntity.class);
        clusteredService.deprecate(deprecatedAccession, "deprecated for testing");
        String getVariantUrl = URL + deprecatedAccession;

        // when
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> response =
                testRestTemplate.exchange(getVariantUrl, HttpMethod.GET, null, new ClusteredVariantType());

        // then
        assertEquals(HttpStatus.GONE, response.getStatusCode());
        assertEquals(1, response.getBody().size());
        assertEquals(variant1, response.getBody().get(0).getData());
        assertClusteredVariantCreatedDateNotNull(response.getBody());
    }

    @Test
    public void testGetByIdFields() {
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse =
                controller.getByIdFields(clusteredVariantEntity1.getAssemblyAccession(),
                                         clusteredVariantEntity1.getContig(),
                                         clusteredVariantEntity1.getStart(),
                                         clusteredVariantEntity1.getType(),
                                         ContigNamingConvention.INSDC);

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(clusteredVariantEntity1.getAccession(), getVariantsResponse.getBody().get(0).getAccession());
    }

    @Test
    public void testGetByIdFields_withContigTranslation() {
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse =
                controller.getByIdFields(clusteredVariantEntity1.getAssemblyAccession(),
                                         clusteredVariantEntity1.getContig() + ENA_CONTIG_SUFFIX,
                                         clusteredVariantEntity1.getStart(),
                                         clusteredVariantEntity1.getType(),
                                         ContigNamingConvention.ENA_SEQUENCE_NAME);

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(clusteredVariantEntity1.getAccession(), getVariantsResponse.getBody().get(0).getAccession());
        assertEquals(clusteredVariantEntity1.getContig() + ENA_CONTIG_SUFFIX,
                     getVariantsResponse.getBody().get(0).getData().getContig());
    }

    @Test
    public void testGetByIdFields_withWrongContigTranslation() {
        assertThrows(ResponseStatusException.class,
                     () -> controller.getByIdFields(clusteredVariantEntity1.getAssemblyAccession(),
                                                    clusteredVariantEntity1.getContig(),
                                                    clusteredVariantEntity1.getStart(),
                                                    clusteredVariantEntity1.getType(),
                                                    ContigNamingConvention.UCSC));
    }

    @Test
    public void testGetByIdFieldsHumanVariant() {
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse =
                controller.getByIdFields(clusteredHumanVariantEntity1.getAssemblyAccession(),
                                         clusteredHumanVariantEntity1.getContig(),
                                         clusteredHumanVariantEntity1.getStart(),
                                         clusteredHumanVariantEntity1.getType(),
                                         ContigNamingConvention.INSDC);

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(clusteredHumanVariantEntity1.getAccession(), getVariantsResponse.getBody().get(0).getAccession());
    }

    @Test
    public void testGetByIdFieldsHumanVariantInOperations() {
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse =
                controller.getByIdFields(clusteredHumanVariantEntity3.getAssemblyAccession(),
                                         clusteredHumanVariantEntity3.getContig(),
                                         clusteredHumanVariantEntity3.getStart(),
                                         clusteredHumanVariantEntity3.getType(),
                                         ContigNamingConvention.INSDC);

        assertEquals(HttpStatus.OK, getVariantsResponse.getStatusCode());
        assertEquals(clusteredHumanVariantEntity3.getAccession(), getVariantsResponse.getBody().get(0).getAccession());
    }

    @Test
    public void testGetByIdFieldsHumanVariantDoesntExists() {
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse =
                controller.getByIdFields(clusteredHumanVariantEntity3.getAssemblyAccession(),
                                         clusteredHumanVariantEntity3.getContig(),
                                         1L,
                                         clusteredHumanVariantEntity3.getType(),
                                         ContigNamingConvention.INSDC);

        assertEquals(HttpStatus.NOT_FOUND, getVariantsResponse.getStatusCode());
    }

    @Test
    public void testGetByIdFieldsClusteredVariantDoesntExists() {
        ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> getVariantsResponse =
                controller.getByIdFields(clusteredVariantEntity1.getAssemblyAccession(),
                                         clusteredVariantEntity1.getContig(),
                                         123,
                                         clusteredVariantEntity1.getType(),
                                         ContigNamingConvention.INSDC);

        assertEquals(HttpStatus.NOT_FOUND, getVariantsResponse.getStatusCode());
    }

    @Test(expected = RuntimeException.class)
    public void getByIdFieldstError500() {
        String assemblyId = "GCA_ERROR";
        String chromosome = "CHROM1";
        int start = 123;
        mockController.getByIdFields(assemblyId, chromosome, start, VariantType.SNV, ContigNamingConvention.INSDC);
    }

    @Test
    public void doesVariantExistTrueWithDatasets() {
        HttpServletResponse response = new MockHttpServletResponse();
        BeaconAlleleResponse beaconAlleleResponse = controller.doesVariantExist(
                clusteredVariantEntity1.getAssemblyAccession(),
                clusteredVariantEntity1.getContig() + ENA_CONTIG_SUFFIX,
                clusteredVariantEntity1.getStart(),
                clusteredVariantEntity1.getType(),
                true,
                response);

        assertTrue(beaconAlleleResponse.isExists());
        assertDatasets(beaconAlleleResponse);
        assertEmbeddedAlleleRequest(beaconAlleleResponse, clusteredVariantEntity1);
    }

    private void assertDatasets(BeaconAlleleResponse beaconAlleleResponse) {
        assertNotNull(beaconAlleleResponse.getDatasetAlleleResponses());

        BeaconDatasetAlleleResponse datasetAlleleResponse = beaconAlleleResponse.getDatasetAlleleResponses().get(0);
        assertEquals("PROJECT1", datasetAlleleResponse.getDatasetId());

        KeyValuePair rsInfo = datasetAlleleResponse.getInfo().get(0);
        assertEquals("RS ID", rsInfo.getKey());
        assertEquals("rs1", rsInfo.getValue());

        KeyValuePair ssInfo = datasetAlleleResponse.getInfo().get(1);
        assertEquals("SS IDs", ssInfo.getKey());
        assertEquals("ss11", ssInfo.getValue());
    }

    private void assertEmbeddedAlleleRequest(BeaconAlleleResponse beaconAlleleResponse,
                                             DbsnpClusteredVariantEntity clusteredVariantEntity) {
        assertEmbeddedAlleleRequest(beaconAlleleResponse, clusteredVariantEntity.getAssemblyAccession(),
                                    clusteredVariantEntity.getStart(), clusteredVariantEntity.getType());
    }

    private void assertEmbeddedAlleleRequest(BeaconAlleleResponse beaconAlleleResponse, String assemblyId,
                                             long start, VariantType variantType) {
        BeaconAlleleRequest alleleRequest = beaconAlleleResponse.getAlleleRequest();
        assertEquals(alleleRequest.getAssemblyId(), assemblyId);
        //The chromosome assert would fail because its not one of the Chromosome enum values, for now the allele
        //request sent in the response will not have that value if its different from the enum
        //assertEquals(alleleRequest.getReferenceName().toString(), clusteredVariantEntity1.getContig());
        assertNull(alleleRequest.getReferenceName());
        assertEquals(start, (long) alleleRequest.getStart());
        assertEquals(variantType.toString(), alleleRequest.getVariantType());
    }

    @Test
    public void doesVariantExistTrueWithoutDatasets() {
        HttpServletResponse response = new MockHttpServletResponse();
        BeaconAlleleResponse beaconAlleleResponse = controller.doesVariantExist(
                clusteredVariantEntity1.getAssemblyAccession(),
                clusteredVariantEntity1.getContig() + ENA_CONTIG_SUFFIX,
                clusteredVariantEntity1.getStart(),
                clusteredVariantEntity1.getType(),
                false,
                response);

        assertTrue(beaconAlleleResponse.isExists());
        assertNull(beaconAlleleResponse.getDatasetAlleleResponses());
        assertEmbeddedAlleleRequest(beaconAlleleResponse, clusteredVariantEntity1);
    }

    @Test
    public void doesVariantExistFalse() {
        HttpServletResponse response = new MockHttpServletResponse();
        BeaconAlleleResponse beaconAlleleResponse = controller.doesVariantExist(
                clusteredVariantEntity1.getAssemblyAccession(),
                clusteredVariantEntity1.getContig() + ENA_CONTIG_SUFFIX,
                123L,
                clusteredVariantEntity1.getType(),
                false,
                response);

        assertFalse(beaconAlleleResponse.isExists());
        assertNull(beaconAlleleResponse.getDatasetAlleleResponses());
        assertEmbeddedAlleleRequest(beaconAlleleResponse, clusteredVariantEntity1.getAssemblyAccession(), 123L,
                                    clusteredVariantEntity1.getType());
    }

    @Test
    public void doesVariantExistError500() {
        HttpServletResponse response = new MockHttpServletResponse();
        String assemblyId = "GCA_ERROR";
        String chromosome = "CHROM1";
        int start = 123;
        BeaconAlleleResponse beaconAlleleResponse = mockController
                .doesVariantExist(assemblyId, chromosome, start, VariantType.SNV, false, response);

        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response.getStatus());
        assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                     (int) beaconAlleleResponse.getError().getErrorCode());
        assertEmbeddedAlleleRequest(beaconAlleleResponse, assemblyId, start, VariantType.SNV);
    }
}
