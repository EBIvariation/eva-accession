/*
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
 */
package uk.ac.ebi.eva.accession.core.service.nonhuman;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionSaveMode;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.HashAlreadyExistsException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.generators.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.model.ISubmittedVariant.DEFAULT_VALIDATED;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:ss-accession-test.properties")
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class SubmittedVariantAccessioningServiceTest {
    private static String TEST_APPLICATION_INSTANCE_ID = "test-application-instance-id";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean ALLELES_MATCH = false;

    private static final String PROJECT = "PRJEB21794";

    private static final String PROJECT_DBSNP = "DBSNP999";

    private static final long ACCESSION = 5000000000L;

    private static final long ACCESSION_TO_DEPRECATE = 5000000002L;

    private static final long ACCESSION_TO_MERGE_1 = 5000000004L;

    private static final long ACCESSION_TO_MERGE_2 = 5000000005L;

    private static final long ACCESSION_DBSNP_1 = 2200000000L;

    private static final long ACCESSION_DBSNP_2 = 2200000001L;

    private static final String DEPRECATE_REASON = "Test deprecate";

    private static final String MERGE_REASON = "Test merge";

    private SubmittedVariant submittedVariant;

    private SubmittedVariant newSubmittedVariant;

    private SubmittedVariant dbsnpSubmittedVariant;

    private SubmittedVariant submittedVariantModified;

    @Autowired
    private DbsnpMonotonicAccessionGenerator<ISubmittedVariant> dbsnpAccessionGenerator;

    @Autowired
    private DbsnpSubmittedVariantAccessioningDatabaseService dbServiceDbsnp;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("submitted-variants-test").build());

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Autowired
    private SubmittedVariantInactiveService inactiveService;

    @Autowired
    private DbsnpSubmittedVariantInactiveService dbsnpInactiveService;

    @Autowired
    private Long accessioningMonotonicInitSs;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Autowired
    private MongoDbFactory mongoDbFactory;


    @Before
    public void setUp() {
        submittedVariant = new SubmittedVariant("GCA_000003055.3", 9913, PROJECT, "21", 20800319, "C", "T",
                                                CLUSTERED_VARIANT, DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                DEFAULT_ASSEMBLY_MATCH, true,
                                                DEFAULT_VALIDATED, null);

        newSubmittedVariant = new SubmittedVariant("assembly", 1111, "project", "contig_2", 100, "ref", "alt",
                                                   CLUSTERED_VARIANT, DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                   DEFAULT_ASSEMBLY_MATCH, true,
                                                   DEFAULT_VALIDATED, null);

        dbsnpSubmittedVariant = new SubmittedVariant("GCA_000009999.3", 9999, PROJECT_DBSNP, "21", 20849999, "", "GG",
                                                     CLUSTERED_VARIANT, DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                     DEFAULT_ASSEMBLY_MATCH, true,
                                                     DEFAULT_VALIDATED, null);

        submittedVariantModified = new SubmittedVariant("GCA_000003055.3", 9913, PROJECT, "21", 20800319,
                                                        "C", "TCTC", CLUSTERED_VARIANT, DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                        DEFAULT_ASSEMBLY_MATCH, true, DEFAULT_VALIDATED, null);
//        submittedVariantModified.setCreatedDate(LocalDateTime.now());
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void sameAccessionsAreReturnedForIdenticalVariants() throws AccessionCouldNotBeGeneratedException {
        List<SubmittedVariant> variants = Arrays.asList(
                new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref", "alt", CLUSTERED_VARIANT,
                                     DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH, ALLELES_MATCH,
                                     DEFAULT_VALIDATED, null),
                newSubmittedVariant);

        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreate(variants, TEST_APPLICATION_INSTANCE_ID);
        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> retrievedAccessions = service.getOrCreate(variants, TEST_APPLICATION_INSTANCE_ID);

        assertEquals(new HashSet<>(generatedAccessions), new HashSet<>(retrievedAccessions));
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void sameAccessionsAreReturnedForEquivalentVariants() throws AccessionCouldNotBeGeneratedException {
        List<SubmittedVariant> originalVariants = Arrays.asList(
                new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref", "alt", 10L,
                                     DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH, ALLELES_MATCH,
                                     DEFAULT_VALIDATED, null),
                newSubmittedVariant);
        List<SubmittedVariant> requestedVariants = Arrays.asList(
                new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref", "alt", null),
                new SubmittedVariant("assembly", 1111, "project", "contig_2", 100, "ref", "alt", null));
        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreate(
                originalVariants, TEST_APPLICATION_INSTANCE_ID);
        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> retrievedAccessions = service.getOrCreate(
                requestedVariants, TEST_APPLICATION_INSTANCE_ID);

        assertEquals(new HashSet<>(generatedAccessions), new HashSet<>(retrievedAccessions));
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json", "/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getOrCreateAccessionsInBothRepositories() throws AccessionCouldNotBeGeneratedException {
        List<SubmittedVariant> variants = Arrays.asList(submittedVariant, newSubmittedVariant, dbsnpSubmittedVariant);
        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants = service.getOrCreate(variants, TEST_APPLICATION_INSTANCE_ID);

        long accession = submittedVariants.stream()
                                          .filter(x -> x.getData().getProjectAccession().equals(PROJECT))
                                          .findAny().orElseThrow(NoSuchElementException::new)
                                          .getAccession();
        assertEquals(ACCESSION, accession);

        long accessionDbsnp = submittedVariants.stream()
                                               .filter(x -> x.getData().getProjectAccession().equals(PROJECT_DBSNP))
                                               .findAny().orElseThrow(NoSuchElementException::new)
                                               .getAccession();
        assertEquals(ACCESSION_DBSNP_1, accessionDbsnp);

        assertEquals(3, submittedVariants.size());
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getOrCreateDbsnpIdenticalVariants() throws AccessionCouldNotBeGeneratedException {
        SubmittedVariant variantPresentInDbsnp = new SubmittedVariant("GCA_000009999.3", 9999, PROJECT_DBSNP, "21",
                                                                      20849999, "", "GG", null);

        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreate(
                Collections.singletonList(variantPresentInDbsnp), TEST_APPLICATION_INSTANCE_ID);
        assertEquals(Collections.singleton(ACCESSION_DBSNP_1),
                     generatedAccessions.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet()));
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getOrCreateDbsnpEquivalentVariants() throws AccessionCouldNotBeGeneratedException {
        SubmittedVariant identicalVariantPresentInDbsnp = new SubmittedVariant("GCA_000009999.3", 9999, PROJECT_DBSNP,
                                                                               "21", 20849999, "", "GG", null);

        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreate(
                Arrays.asList(identicalVariantPresentInDbsnp), TEST_APPLICATION_INSTANCE_ID);

        assertEquals(Collections.singleton(ACCESSION_DBSNP_1),
                     generatedAccessions.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet()));

        int differentTaxonomy = 1000;
        SubmittedVariant equivalentVariantPresentInDbsnp = new SubmittedVariant("GCA_000009999.3", differentTaxonomy,
                                                                                PROJECT_DBSNP, "21", 20849999, "", "GG",
                                                                                null);

        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions2 = service.getOrCreate(
                Arrays.asList(identicalVariantPresentInDbsnp, equivalentVariantPresentInDbsnp), TEST_APPLICATION_INSTANCE_ID);

        assertEquals(Collections.singleton(ACCESSION_DBSNP_1),
                     generatedAccessions2.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet()));
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json", "/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getFromBothRepositories() {
        List<SubmittedVariant> variants = Arrays.asList(submittedVariant, newSubmittedVariant, dbsnpSubmittedVariant);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(variants);
        assertEquals(2, accessions.size());
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json", "/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getByAccessionsFromBothRepositories()
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        AccessionWrapper<ISubmittedVariant, String, Long> retrievedSubmittedVariant = service.getByAccession(ACCESSION);
        assertEquals(submittedVariant, retrievedSubmittedVariant.getData());
        AccessionWrapper<ISubmittedVariant, String, Long> retrievedSubmittedDbsnpVariant = service.getByAccession(
                ACCESSION_DBSNP_1);
        assertEquals(dbsnpSubmittedVariant, retrievedSubmittedDbsnpVariant.getData());
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getAllByAccession() throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        assertEquals(2, service.getAllByAccession(2200000002L).size());
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getAllByIdFields() {
        assertEquals(2, service.getAllByIdFields("GCA_000009999.3", "21", Arrays.asList("DBSNP999", "DBSNP111"),
                                                 20849999L, "", "GG", ContigNamingConvention.INSDC).size());
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json", "/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getByAccessionAndVersionFromBothRepositories() throws AccessionMergedException,
            AccessionDoesNotExistException, AccessionDeprecatedException {
        AccessionWrapper<ISubmittedVariant, String, Long> retrievedSubmittedVariant = service.getByAccessionAndVersion(
                ACCESSION, 1);
        assertEquals(submittedVariant, retrievedSubmittedVariant.getData());

        AccessionWrapper<ISubmittedVariant, String, Long> retrievedDbsnpSubmittedVariant =
                service.getByAccessionAndVersion(ACCESSION_DBSNP_1, 1);
        assertEquals(dbsnpSubmittedVariant, retrievedDbsnpSubmittedVariant.getData());
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    @Test
    public void updateSubmittedVariant() throws AccessionDeprecatedException, AccessionDoesNotExistException,
            AccessionMergedException, HashAlreadyExistsException {
        service.update(ACCESSION, 1, submittedVariantModified);
        assertVariantUpdated(ACCESSION, submittedVariantModified);
    }

    private void assertVariantUpdated(long accession, ISubmittedVariant modifiedVariant)
            throws AccessionDeprecatedException, AccessionDoesNotExistException, AccessionMergedException {
        assertEquals(modifiedVariant, service.getByAccessionAndVersion(accession, 1).getData());

        IEvent<ISubmittedVariant, Long> lastOperation;
        if (accession >= accessioningMonotonicInitSs) {
            lastOperation = inactiveService.getLastEvent(accession);
        } else {
            lastOperation = dbsnpInactiveService.getLastEvent(accession);
        }
        assertEquals(EventType.UPDATED, lastOperation.getEventType());
        assertEquals(accession, lastOperation.getAccession().longValue());
        assertNull(lastOperation.getMergedInto());
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void updateDbsnpSubmittedVariant() throws AccessionDeprecatedException, AccessionDoesNotExistException,
            AccessionMergedException, HashAlreadyExistsException {
        service.update(ACCESSION_DBSNP_1, 1, submittedVariantModified);
        assertVariantUpdated(ACCESSION_DBSNP_1, submittedVariantModified);
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    @Test
    public void patchSubmittedVariant() throws AccessionDeprecatedException, AccessionDoesNotExistException,
            AccessionMergedException, HashAlreadyExistsException {
        service.patch(ACCESSION, submittedVariantModified);
        assertVariantPatched(ACCESSION, submittedVariantModified);
    }

    private void assertVariantPatched(long accession, ISubmittedVariant modifiedVariant)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        ISubmittedVariant variantFromDb = service.getByAccession(accession).getData();
        assertEquals(modifiedVariant, variantFromDb);
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void patchDbsnpSubmittedVariant() throws AccessionDeprecatedException, AccessionDoesNotExistException,
            AccessionMergedException, HashAlreadyExistsException {
        service.patch(ACCESSION_DBSNP_1, submittedVariantModified);
        assertVariantPatched(ACCESSION_DBSNP_1, submittedVariantModified);
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    @Test
    public void deprecateSubmittedVariant() throws AccessionMergedException, AccessionDoesNotExistException,
            AccessionDeprecatedException {
        assertNotNull(service.getByAccession(ACCESSION_TO_DEPRECATE));
        service.deprecate(ACCESSION_TO_DEPRECATE, DEPRECATE_REASON);
        assertVariantDeprecated(ACCESSION_TO_DEPRECATE, DEPRECATE_REASON);
    }

    private void assertVariantDeprecated(long accession, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        IEvent<ISubmittedVariant, Long> lastOperation;
        if (accession >= accessioningMonotonicInitSs) {
            lastOperation = inactiveService.getLastEvent(accession);
        } else {
            lastOperation = dbsnpInactiveService.getLastEvent(accession);
        }
        assertEquals(EventType.DEPRECATED, lastOperation.getEventType());
        assertEquals(accession, lastOperation.getAccession().longValue());
        assertNull(lastOperation.getMergedInto());
        assertEquals(reason, lastOperation.getReason());

        expectedException.expect(AccessionDeprecatedException.class);
        service.getByAccession(accession);
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void deprecateDnsnpSubmittedVariant() throws AccessionMergedException, AccessionDoesNotExistException,
            AccessionDeprecatedException {
        assertNotNull(service.getByAccession(ACCESSION_DBSNP_1));
        service.deprecate(ACCESSION_DBSNP_1, DEPRECATE_REASON);
        assertVariantDeprecated(ACCESSION_DBSNP_1, DEPRECATE_REASON);
    }

    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    @Test
    public void mergeSubmittedVariant() throws AccessionMergedException, AccessionDoesNotExistException,
            AccessionDeprecatedException {
        assertNotNull(service.getByAccession(ACCESSION_TO_MERGE_1));
        assertNotNull(service.getByAccession(ACCESSION_TO_MERGE_2));
        service.merge(ACCESSION_TO_MERGE_1, ACCESSION_TO_MERGE_2, MERGE_REASON);
        assertVariantMerged(ACCESSION_TO_MERGE_1, ACCESSION_TO_MERGE_2, MERGE_REASON);
    }

    private void assertVariantMerged(long accessionOrigin, long accessionDestination, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        IEvent<ISubmittedVariant, Long> lastOperation;
        if (accessionOrigin >= accessioningMonotonicInitSs) {
            lastOperation = inactiveService.getLastEvent(accessionOrigin);
        } else {
            lastOperation = dbsnpInactiveService.getLastEvent(accessionOrigin);
        }
        assertEquals(EventType.MERGED, lastOperation.getEventType());
        assertEquals(accessionOrigin, lastOperation.getAccession().longValue());
        assertEquals(accessionDestination, lastOperation.getMergedInto().longValue());
        assertEquals(MERGE_REASON, lastOperation.getReason());

        assertNotNull(service.getByAccession(accessionDestination));

        expectedException.expect(AccessionMergedException.class);
        service.getByAccession(accessionOrigin);
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void mergeDbsnpSubmittedVariant() throws AccessionMergedException, AccessionDoesNotExistException,
            AccessionDeprecatedException {
        assertNotNull(service.getByAccession(ACCESSION_DBSNP_1));
        assertNotNull(service.getByAccession(ACCESSION_DBSNP_2));
        service.merge(ACCESSION_DBSNP_1, ACCESSION_DBSNP_2, MERGE_REASON);
        assertVariantMerged(ACCESSION_DBSNP_1, ACCESSION_DBSNP_2, MERGE_REASON);
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test(expected = UnsupportedOperationException.class)
    public void exceptionWhenCreateAccessionForDbsnpVariant() throws AccessionCouldNotBeGeneratedException {
        DbsnpSubmittedVariantMonotonicAccessioningService accessioningServiceDbsnp =
                new DbsnpSubmittedVariantMonotonicAccessioningService(
                        dbsnpAccessionGenerator, dbServiceDbsnp, new SubmittedVariantSummaryFunction(),
                        new SHA1HashingFunction(), AccessionSaveMode.SAVE_ALL_THEN_RESOLVE);

        accessioningServiceDbsnp.getOrCreate(Collections.singletonList(submittedVariant), TEST_APPLICATION_INSTANCE_ID);
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void defaultValuesAreNotStored() throws AccessionCouldNotBeGeneratedException {
        boolean NOT_DEFAULT_SUPPORTED_BY_EVIDENCE = !DEFAULT_SUPPORTED_BY_EVIDENCE;
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref",
                                                                 "alt", CLUSTERED_VARIANT,
                                                                 NOT_DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                 DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH,
                                                                 DEFAULT_VALIDATED, null);

        service.getOrCreate(Collections.singletonList(submittedVariant), TEST_APPLICATION_INSTANCE_ID);


        List<Document> submittedVariantEntities  = new ArrayList<>();
        MongoCursor<Document> submittedVariantEntitiesCursor = mongoDbFactory.getDb()
                                                                             .getCollection("submittedVariantEntity")
                                                                             .find().iterator();

        submittedVariantEntitiesCursor.forEachRemaining(submittedVariantEntities::add);
        assertEquals(NOT_DEFAULT_SUPPORTED_BY_EVIDENCE, submittedVariantEntities.get(0).get("evidence"));
        assertEquals(null, submittedVariantEntities.get(0).get("asmMatch"));
        assertEquals(null, submittedVariantEntities.get(0).get("allelesMatch"));
        assertEquals(null, submittedVariantEntities.get(0).get("validated"));
    }

    @UsingDataSet(locations = {"/test-data/dbsnpSubmittedVariantEntity.json"})
    @Test
    public void getSeveralVariantsWithSameAccession() {
        SubmittedVariant multiallelicSubmittedVariant1 = new SubmittedVariant("GCA_000009999.3", 9999, "DBSNP555",
                                                                              "21", 20849999, "A", "G", 2200000002L,
                                                                              DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                              DEFAULT_ASSEMBLY_MATCH,
                                                                              true, DEFAULT_VALIDATED, null);

        SubmittedVariant multiallelicSubmittedVariant2 = new SubmittedVariant("GCA_000009999.3", 9999, "DBSNP555",
                                                                              "21", 20849999, "A", "C", 2200000003L,
                                                                              DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                              DEFAULT_ASSEMBLY_MATCH,
                                                                              true, DEFAULT_VALIDATED, null);

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(
                Arrays.asList(multiallelicSubmittedVariant1, multiallelicSubmittedVariant2));
        assertEquals(2, accessions.size());
        assertEquals(1, accessions.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet()).size());
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void createdDateIsReturnedInAccessionedVariant() throws AccessionCouldNotBeGeneratedException {
        LocalDateTime createdDate = LocalDateTime.of(2018, Month.SEPTEMBER, 18, 9, 0);

        submittedVariant.setCreatedDate(createdDate);

        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions =
                service.getOrCreate(Collections.singletonList(submittedVariant), TEST_APPLICATION_INSTANCE_ID);

        assertEquals(createdDate, generatedAccessions.get(0).getData().getCreatedDate());
    }

}
