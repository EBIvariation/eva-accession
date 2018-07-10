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

package uk.ac.ebi.eva.accession.core;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.core.LoadStrategyEnum;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;

import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:ss-accession-test.properties")
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class SubmittedVariantAccessioningServiceTest {

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = true;

    private static final Boolean MATCHES_ASSEMBLY = true;

    private static final Boolean ALLELES_MATCH = false;

    private static final Boolean VALIDATED = false;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("submitted-variants-test").build());

    @Autowired
    private SubmittedVariantAccessioningService service;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;


    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void sameAccessionsAreReturnedForIdenticalVariants() throws AccessionCouldNotBeGeneratedException {
        List<SubmittedVariant> variants = Arrays.asList(
                new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref", "alt", CLUSTERED_VARIANT,
                                     SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED),
                new SubmittedVariant("assembly", 1111, "project", "contig_2", 100, "ref", "alt", CLUSTERED_VARIANT,
                                     SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED));
        List<AccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreate(variants);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> retrievedAccessions = service.getOrCreate(variants);

        assertEquals(new HashSet<>(generatedAccessions), new HashSet<>(retrievedAccessions));
    }

    @UsingDataSet(loadStrategy = LoadStrategyEnum.DELETE_ALL)
    @Test
    public void sameAccessionsAreReturnedForEquivalentVariants() throws AccessionCouldNotBeGeneratedException {
        List<SubmittedVariant> originalVariants = Arrays.asList(
                new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref", "alt", CLUSTERED_VARIANT,
                                     SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED),
                new SubmittedVariant("assembly", 1111, "project", "contig_2", 100, "ref", "alt", CLUSTERED_VARIANT,
                                     SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED));
        List<SubmittedVariant> requestedVariants = Arrays.asList(
                new SubmittedVariant("assembly", 1111, "project", "contig_1", 100, "ref", "alt", null, null, null, null,
                                     null),
                new SubmittedVariant("assembly", 1111, "project", "contig_2", 100, "ref", "alt", null, null, null, null,
                                     null));
        List<AccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreate(
                originalVariants);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> retrievedAccessions = service.getOrCreate(
                requestedVariants);

        assertEquals(new HashSet<>(generatedAccessions),
                     new HashSet<>(retrievedAccessions));
    }
}
