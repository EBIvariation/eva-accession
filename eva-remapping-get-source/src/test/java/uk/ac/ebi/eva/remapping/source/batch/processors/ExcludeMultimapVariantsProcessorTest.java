/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.source.batch.processors;

import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

import java.util.function.Function;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ExcludeMultimapVariantsProcessorTest {

    private Function<ISubmittedVariant, String> submittedHashingFunction;

    private ExcludeMultimapVariantsProcessor processor;

    @Before
    public void setUp() {
        processor = new ExcludeMultimapVariantsProcessor();
        submittedHashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Test
    public void dontExcludeVariantWhenMultimapNull() throws Exception {
        Integer mapWeight = null;
        SubmittedVariantEntity submittedVariantEntity = createSubmittedVariantEntity(mapWeight);
        assertNotNull(processor.process(submittedVariantEntity));
    }

    @Test
    public void dontExcludeVariantWhenMultimapIsOne() throws Exception {
        Integer mapWeight = 1;
        SubmittedVariantEntity submittedVariantEntity = createSubmittedVariantEntity(mapWeight);
        assertNotNull(processor.process(submittedVariantEntity));
    }

    @Test
    public void excludeMultimapVariant() throws Exception {
        Integer mapWeight = 2;
        SubmittedVariantEntity submittedVariantEntity = createSubmittedVariantEntity(mapWeight);
        assertNull(processor.process(submittedVariantEntity));
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(Integer mapWeight) {
        SubmittedVariant submittedVariant = new SubmittedVariant("asm", 1000, "project", "contig", 100, "A", "T", null);
        String hash = submittedHashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(5000000000L, hash, "asm", 1000,
                                                                                   "project", "contig", 100, "A", "T",
                                                                                   null, false, false, false, false, 1,
                                                                                   mapWeight);
        return submittedVariantEntity;
    }
}