/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.repository.nonhuman.eva;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.AccessionProjection;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:ss-accession-test.properties")
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class ClusteredVariantAccessioningRepositoryTest {

    private ClusteredVariant clusteredVariant;

    private ClusteredVariant newClusteredVariant;

    @Autowired
    private ClusteredVariantAccessioningRepository repository;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("submitted-variants-test").build());

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Before
    public void setUp() {
        clusteredVariant = new ClusteredVariant("GCA_000003055.3", 9913, "21", 20800319, VariantType.SNV, false,
                                                LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

        newClusteredVariant = new ClusteredVariant("assembly", 1111, "contig_2", 100, VariantType.SNV, false,
                                                   LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void queryAccessionRange() {
        long firstAccession = 1000L;
        long secondAccession = 1002L;
        List<ClusteredVariantEntity> variants = Arrays.asList(
                new ClusteredVariantEntity(firstAccession, "hash-1", clusteredVariant, 1),
                new ClusteredVariantEntity(secondAccession, "hash-2", newClusteredVariant, 1));

        repository.saveAll(variants);

        assertAccessionsEquals(Arrays.asList(firstAccession),
                               repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1000L, 1000L));

        assertAccessionsEquals(Arrays.asList(),
                               repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1001L, 1001L));

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

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void testQueryAssemblyAndAccessionFilter() {
        long firstAccession = 1000L;
        long secondAccession = 1002L;
        List<ClusteredVariantEntity> variants = Arrays.asList(
                new ClusteredVariantEntity(firstAccession, "hash-1", clusteredVariant, 1),
                new ClusteredVariantEntity(secondAccession, "hash-2", newClusteredVariant, 1));

        repository.saveAll(variants);

        List<Long> accessions =  repository.findByAssemblyAccessionAndAccessionIn(
                "assembly", Arrays.asList(firstAccession, secondAccession)
        ).stream().map(AccessionedDocument::getAccession).collect(Collectors.toList());
        assertEquals(1, accessions.size());

        accessions =  repository.findByAssemblyAccessionAndAccessionIn(
                "assembly", Collections.singletonList(firstAccession)
        ).stream().map(AccessionedDocument::getAccession).collect(Collectors.toList());
        assertEquals(0, accessions.size());
    }
}
