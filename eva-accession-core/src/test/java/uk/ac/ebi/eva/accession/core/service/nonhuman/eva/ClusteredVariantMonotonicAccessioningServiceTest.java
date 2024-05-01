/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.service.nonhuman.eva;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:rs-accession-test.properties")
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class})
public class ClusteredVariantMonotonicAccessioningServiceTest {

    @Autowired
    private ClusteredVariantMonotonicAccessioningService clusteredVariantMonotonicAccessioningService;

    @Test
    public void service() throws AccessionCouldNotBeGeneratedException {
        ClusteredVariant clusteredVariant1 = new ClusteredVariant("asm1", 100, "chr1", 1, VariantType.SNV, false, null);
        ClusteredVariant clusteredVariant2 = new ClusteredVariant("asm1", 100, "chr1", 5, VariantType.SNV, false, null);
        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionedWrappers =
                clusteredVariantMonotonicAccessioningService.getOrCreate(Arrays.asList(clusteredVariant1,
                                                                                       clusteredVariant2), "test-application-instance-id");
        assertEquals(2, accessionedWrappers.size());
        assertTrue(isAccessionInResults(accessionedWrappers, 3000000000L));
        assertTrue(isAccessionInResults(accessionedWrappers, 3000000001L));
    }

    private boolean isAccessionInResults(List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> results,
                                         Long accession) {
        return results.stream().filter(a -> a.getAccession().equals(accession)).count() == 1;
    }
}