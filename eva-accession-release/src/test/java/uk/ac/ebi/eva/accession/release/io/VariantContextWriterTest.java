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
package uk.ac.ebi.eva.accession.release.io;

import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.accession.release.steps.processors.VariantToVariantContextProcessor;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.File;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class VariantContextWriterTest {

    private static final String ID = "rs123";

    private static final String CHR_1 = "1";

    private static final String FILE_ID = "fileId";

    private static final String SNP_SEQUENCE_ONTOLOGY = "SO:0001483";

    private static final String VARIANT_CLASS_KEY = "VC";

    private static final String STUDY_ID_KEY = "SID";

    private static final String STUDY_1 = "study_1";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void basicWrite() throws Exception {
        File output = temporaryFolder.newFile("test.vcf");
        VariantContextWriter writer = new VariantContextWriter(output);
        Variant variant = buildVariant(CHR_1, 1000, "C", "A", SNP_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = new VariantToVariantContextProcessor().process(variant);
        writer.open(null);
        writer.write(Collections.singletonList(variantContext));
        writer.close();
        assertTrue(output.exists());
    }

    private Variant buildVariant(String chr1, int start, String reference, String alternate, String sequenceOntology,
                                 String... studies) {
        Variant variant = new Variant(chr1, start, start + alternate.length(), reference, alternate);
        variant.setMainId(ID);
        for (String study : studies) {
            VariantSourceEntry sourceEntry = new VariantSourceEntry(study, FILE_ID);
            sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntology);
            sourceEntry.addAttribute(STUDY_ID_KEY, study);
            variant.addSourceEntry(sourceEntry);
        }
        return variant;
    }
}