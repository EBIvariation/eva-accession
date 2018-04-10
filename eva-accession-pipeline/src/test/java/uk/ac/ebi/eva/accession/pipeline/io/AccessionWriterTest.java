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
package uk.ac.ebi.eva.accession.pipeline.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.AccessionWriterConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@DataJpaTest
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, InputParametersConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class AccessionWriterTest {

    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Autowired
    private InputParameters inputParameters;

    private AccessionWriter accessionWriter;

    @Before
    public void setUp() throws Exception {
        accessionWriter = new AccessionWriter(service);
    }

    @Test
    public void saveSingleAccession() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", "contig", 100, "reference",
                                                         "alternate", false);

        accessionWriter.write(Collections.singletonList(variant));

        Map<Long, ISubmittedVariant> accessions = service.getAccessions(Collections.singletonList(variant));
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long)accessions.keySet().iterator().next());

        ISubmittedVariant savedVariant = accessions.values().iterator().next();
        assertEquals(variant.getAssemblyAccession(), savedVariant.getAssemblyAccession());
        assertEquals(variant.getTaxonomyAccession(), savedVariant.getTaxonomyAccession());
        assertEquals(variant.getProjectAccession(), savedVariant.getProjectAccession());
        assertEquals(variant.getContig(), savedVariant.getContig());
        assertEquals(variant.getStart(), savedVariant.getStart());
        assertEquals(variant.getReferenceAllele(), savedVariant.getReferenceAllele());
        assertEquals(variant.getAlternateAllele(), savedVariant.getAlternateAllele());
        assertEquals(variant.isSupportedByEvidence(), savedVariant.isSupportedByEvidence());
    }
}
