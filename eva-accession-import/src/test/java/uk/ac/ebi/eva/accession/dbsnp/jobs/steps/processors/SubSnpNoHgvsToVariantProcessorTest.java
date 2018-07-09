/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.accession.dbsnp.jobs.steps.processors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import java.sql.Date;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class SubSnpNoHgvsToVariantProcessorTest {

    public static final String CHICKEN_ASSEMBLY_5 = "Gallus_gallus-5.0";

    SubSnpNoHgvsToVariantProcessor processor;

    @Before
    public void setUp() throws Exception {
        processor = new SubSnpNoHgvsToVariantProcessor();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void transformSnp() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs("C/T", CHICKEN_ASSEMBLY_5, "CHICKEN_SDAU",
                                                     "JININGBAIRI UNINFECTED", "25", 920114L, "NT_456018",
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     49575L, false, false, "C", Date.valueOf("2017-08-22"), 9031);
        ISubmittedVariant variant = processor.process(subSnpNoHgvs);
        // TODO: project name? batch handle + batch name?
        // TODO: supported by evidence?
        // TODO: validated, match assembly and RS variant accession are being added into PR #28
        SubmittedVariant expectedVariant = new SubmittedVariant(CHICKEN_ASSEMBLY_5, 9031,
                                                                "CHICKEN_SDAU_JININGBAIRI UNINFECTED", "25", 920114,
                                                                "C", "T", false);
        expectedVariant.setCreatedDate(LocalDateTime.parse("2017-08-22T13:22:00"));
        assertEquals(expectedVariant, variant);

    }

    // TODO: alleles in reverse
}