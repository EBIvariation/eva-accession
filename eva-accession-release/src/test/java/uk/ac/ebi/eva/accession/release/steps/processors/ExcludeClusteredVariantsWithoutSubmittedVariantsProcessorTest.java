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
package uk.ac.ebi.eva.accession.release.steps.processors;

import org.junit.Test;

import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExcludeClusteredVariantsWithoutSubmittedVariantsProcessorTest {

    @Test
    public void testClusteredVariantsWithoutSubmittedVariants() {
        ExcludeClusteredVariantsWithoutSubmittedVariantsProcessor processor =
                new ExcludeClusteredVariantsWithoutSubmittedVariantsProcessor();
        Variant variant = new Variant("22", 1, 1, "T", "C");
        variant.setMainId("rs123");
        assertNull(processor.process(variant));
    }

    @Test
    public void testClusteredVariantsWithSubmittedVariants() {
        ExcludeClusteredVariantsWithoutSubmittedVariantsProcessor processor =
                new ExcludeClusteredVariantsWithoutSubmittedVariantsProcessor();
        Variant variant = new Variant("22", 1, 1, "T", "C");
        variant.setMainId("rs123");
        variant.addSourceEntry(new VariantSourceEntry("FID1", "SID1"));
        IVariant processedVariant = processor.process(variant);
        assertNotNull(processedVariant);
        assertEquals(variant, processedVariant);
    }
}
