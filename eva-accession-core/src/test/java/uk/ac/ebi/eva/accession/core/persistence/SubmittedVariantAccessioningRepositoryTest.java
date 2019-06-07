/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.persistence;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.AccessionProjection;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;

import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:ss-accession-test.properties")
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class SubmittedVariantAccessioningRepositoryTest {

    private static final Long CLUSTERED_VARIANT = null;

    private static final String PROJECT = "PRJEB21794";

    private SubmittedVariant submittedVariant;

    private SubmittedVariant newSubmittedVariant;

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("submitted-variants-test").build());

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

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

    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void queryAccessionRange() {
        long firstAccession = 1000L;
        long secondAccession = 1002L;
        List<SubmittedVariantEntity> variants = Arrays.asList(
                new SubmittedVariantEntity(firstAccession, "hash-1", submittedVariant, 1),
                new SubmittedVariantEntity(secondAccession, "hash-2", newSubmittedVariant, 1));

        repository.save(variants);

        assertAccessionsEquals(Arrays.asList(),
                               repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1000L, 1000L));

        assertAccessionsEquals(Arrays.asList(firstAccession),
                               repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1000L, 1001L));

        assertAccessionsEquals(Arrays.asList(firstAccession, secondAccession),
                               repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1000L, 1005L));

        assertAccessionsEquals(Arrays.asList(secondAccession),
                               repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1001L, 1005L));
    }

    private void assertAccessionsEquals(List<Long> expectedAccessions,
                                        List<AccessionProjection<Long>> accessionsProjection) {
        assertEquals(new TreeSet<>(expectedAccessions),
                     accessionsProjection.stream().map(AccessionProjection::getAccession).collect(Collectors.toSet()));
    }
}
