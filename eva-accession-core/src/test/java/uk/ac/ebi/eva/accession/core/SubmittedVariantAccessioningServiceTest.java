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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureTestEntityManager;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;

import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningCustomRepositoryImpl;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.test.MongoTestConfiguration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:accession-test.properties")
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, MongoTestConfiguration.class, SubmittedVariantAccessioningCustomRepositoryImpl.class})
//@AutoConfigureTestEntityManager
//@EnableJpaRepositories
public class SubmittedVariantAccessioningServiceTest {

    @Autowired
    SubmittedVariantAccessioningService service;

    @Test
    public void sameAccessionsAreReturnedForIdenticalVariants() throws AccessionCouldNotBeGeneratedException {
        List<SubmittedVariantEntity> variants = Arrays.asList(
                new SubmittedVariantEntity(null, null, "assembly", 1111,
                                           "project", "contig_1", 100, "ref",
                                           "alt", true),
                new SubmittedVariantEntity(null, null, "assembly", 1111,
                                           "project", "contig_2", 100, "ref",
                                           "alt", true));
        List<AccessionWrapper<ISubmittedVariant, String, Long>> generatedAccessions = service.getOrCreateAccessions(variants);
        List<AccessionWrapper<ISubmittedVariant, String, Long>>  retrievedAccessions = service.getOrCreateAccessions(variants);

        assertEquals(generatedAccessions, retrievedAccessions);
    }
}
