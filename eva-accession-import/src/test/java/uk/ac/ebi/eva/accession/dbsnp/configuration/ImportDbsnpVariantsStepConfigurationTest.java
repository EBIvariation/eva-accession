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
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.test.TestConfiguration;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, TestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class ImportDbsnpVariantsStepConfigurationTest {

    private static final int EXPECTED_SUBMITTED_VARIANTS = 8;

    private static final int EXPECTED_CLUSTERED_VARIANTS = 5;

    private static final String CONTIG = "CM000114.4";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository;

    @Autowired
    private InputParameters inputParameters;

    @Before
    public void setUp() throws Exception {
        submittedVariantRepository.deleteAll();
        clusteredVariantRepository.deleteAll();
    }

    @Test
    @DirtiesContext
    public void executeStep() throws IOException {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(IMPORT_DBSNP_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(EXPECTED_SUBMITTED_VARIANTS, submittedVariantRepository.count());
        assertEquals(EXPECTED_CLUSTERED_VARIANTS, clusteredVariantRepository.count());

        List<DbsnpSubmittedVariantEntity> storedSubmittedVariants = new ArrayList<>();
        submittedVariantRepository.findAll().forEach(storedSubmittedVariants::add);
        List<DbsnpClusteredVariantEntity> storedClusteredVariants = new ArrayList<>();
        clusteredVariantRepository.findAll().forEach(storedClusteredVariants::add);

        checkFlagInSubmittedVariants(storedSubmittedVariants, SubmittedVariantEntity::isAssemblyMatch,
                                     Arrays.asList(26201546L, 1540359250L, 88888888L, 9999999L));
        checkFlagInSubmittedVariants(storedSubmittedVariants, SubmittedVariantEntity::isSupportedByEvidence,
                                     Arrays.asList(26201546L, 25062583L, 25312601L, 27587141L, 88888888L));
        checkFlagInSubmittedVariants(storedSubmittedVariants, SubmittedVariantEntity::isAllelesMatch,
                                     Arrays.asList(26201546L, 1540359250L, 25062583L, 25312601L, 88888888L, 9999999L));
        checkFlagInSubmittedVariants(storedSubmittedVariants, SubmittedVariantEntity::isValidated,
                                     Arrays.asList(26201546L, 25312601L, 27587141L, 88888888L));
        checkFlagInClusteredVariants(storedClusteredVariants, DbsnpClusteredVariantEntity::isValidated,
                                     Arrays.asList(13823349L, 7777777L));
        checkRenormalizedVariant(storedSubmittedVariants, storedClusteredVariants);
        checkClusteredVariantsCreationDate(storedClusteredVariants);
        checkSubmittedVariantsCreationDate(storedSubmittedVariants);
    }

    private void checkFlagInSubmittedVariants(List<DbsnpSubmittedVariantEntity> unfilteredVariants,
                                              Predicate<DbsnpSubmittedVariantEntity> predicate,
                                              List<Long> expectedVariants) {
        Set<Long> variantsToCheck = unfilteredVariants.stream().filter(predicate).map(
                SubmittedVariantEntity::getAccession).collect(Collectors.toSet());
        assertEquals(expectedVariants.size(), variantsToCheck.size());
        assertTrue(variantsToCheck.containsAll(expectedVariants));
    }

    private void checkFlagInClusteredVariants(List<DbsnpClusteredVariantEntity> unfilteredVariants,
                                              Predicate<DbsnpClusteredVariantEntity> predicate,
                                              List<Long> expectedVariants) {
        Set<Long> variantsToCheck = unfilteredVariants.stream().filter(predicate).map(
                DbsnpClusteredVariantEntity::getAccession).collect(Collectors.toSet());
        assertEquals(expectedVariants.size(), variantsToCheck.size());
        assertTrue(variantsToCheck.containsAll(expectedVariants));
    }

    private void checkRenormalizedVariant(List<DbsnpSubmittedVariantEntity> submittedVariants,
                                          List<DbsnpClusteredVariantEntity> clusteredVariants) {
        DbsnpSubmittedVariantEntity submittedVariant = submittedVariants.stream().filter(
                ss -> ss.getAccession().equals(9999999L)).findFirst().get();
        SubmittedVariant expectedVariant = new SubmittedVariant("GCF_000002315.4", 9031, "HANDLE_BATCH", CONTIG, 2,
                                                                "GC", "", 6666666L, false, true, true, false, null);
        assertEquals(expectedVariant, submittedVariant.getModel());
        // the hash should have been recalculated with the new model
        Function<ISubmittedVariant, String> hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        assertEquals(hashingFunction.apply(expectedVariant), submittedVariant.getHashedMessage());

        DbsnpClusteredVariantEntity clusteredVariant = clusteredVariants.stream().filter(
                rs -> rs.getAccession().equals(6666666L)).findFirst().get();
        // the RS coordinates are not modified by renormalisation
        assertEquals(3, clusteredVariant.getStart());
    }

    private void checkClusteredVariantsCreationDate(List<DbsnpClusteredVariantEntity> storedClusteredVariants) {
        assertTrue(storedClusteredVariants.stream().noneMatch(variant -> variant.getCreatedDate() == null));
        DbsnpClusteredVariantEntity clusteredVariant = storedClusteredVariants.stream().
                filter(variant -> variant.getAccession().equals(13823349L)).findFirst().get();
         // rs13823349 creation date is 2004-07-02 16:03:00.000
        assertEquals(LocalDateTime.of(2004, 7, 2, 16, 3), clusteredVariant.getCreatedDate());
    }

    private void checkSubmittedVariantsCreationDate(List<DbsnpSubmittedVariantEntity> storedSubmittedVariants) {
        assertTrue(storedSubmittedVariants.stream().noneMatch(variant -> variant.getCreatedDate() == null));
        DbsnpSubmittedVariantEntity submittedVariant = storedSubmittedVariants.stream().
                filter(variant -> variant.getAccession().equals(27587141L)).findFirst().get();
        // ss27587141 creation date is 2004-06-25 02:52:00.000
        assertEquals(LocalDateTime.of(2004, 6, 25, 2, 52), submittedVariant.getCreatedDate());
    }
}
