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
package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertEquals;

public class VariantProcessorTest {

    private VariantProcessor processor;

    @Before
    public void setUp() {
        processor = new VariantProcessor("assembly", 1111, "project");
    }

    @Test
    public void process() throws Exception {
        Variant variant = new Variant("contig", 1000, 1001, "A", "T");
        SubmittedVariant processed = processor.process(variant);
        SubmittedVariant expected = new SubmittedVariant("assembly", 1111, "project", "contig", 1000, "A", "T", true);
        assertEquals(expected, processed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionContigNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant(null, 11, "project", "contig", 1000, "A", "T", true);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionProjectNull() {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", 11, null, "contig", 1000, "A", "T", true);
    }
}