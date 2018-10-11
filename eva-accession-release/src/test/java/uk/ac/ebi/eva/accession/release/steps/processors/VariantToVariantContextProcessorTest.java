/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.release.steps.processors;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.assertj.core.util.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VariantToVariantContextProcessorTest {

    private static final String FILE_ID = "fileId";

    private static final String SNP_SEQUENCE_ONTOLOGY = "SO:0001483";

    private static final String DELETION_SEQUENCE_ONTOLOGY = "SO:0000159";

    private static final String INSERTION_SEQUENCE_ONTOLOGY = "SO:0000667";

    private static final String CHR_1 = "1";

    private static final String ID = "rs123";

    private static final String STUDY_1 = "study_1";

    private static final String STUDY_2 = "study_2";

    private static final String VARIANT_CLASS_KEY = "VC";

    private static final String STUDY_ID_KEY = "SID";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void singleStudySNV() throws Exception {
        Variant variant = buildVariant(CHR_1, 1000, "C", "A", SNP_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);
        checkVariantContext(variantContext, CHR_1, 1000, 1000, ID, "C", "A");
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

    @Test
    public void singleStudySingleNucleotideInsertion() throws Exception {
        Variant variant = buildVariant(CHR_1, 1100, "T", "TG", SNP_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1100, 1100, ID, "T", "TG");
    }

    @Test
    public void singleStudySeveralNucleotidesInsertion() throws Exception {
        Variant variant = buildVariant(CHR_1, 1100, "T", "TGA", INSERTION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1100, 1100, ID, "T", "TGA");
    }

    @Test
    public void singleStudySingleNucleotideDeletion() throws Exception {
        Variant variant = buildVariant(CHR_1, 1100, "TA", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1100, 1101, ID, "TA", "T");
    }

    @Test
    public void singleStudySeveralNucleotidesDeletion() throws Exception {
        Variant variant = buildVariant(CHR_1, 1100, "TAG", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1100, 1102, ID, "TAG", "T");
    }

    @Test
    public void singleStudyMultiAllelicVariant() throws Exception {
        Variant variant = buildVariant(CHR_1, 1100, "C", "A,T", SNP_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();

        expectedException.expect(IllegalArgumentException.class);
        variantConverter.process(variant);
    }
    @Test
    public void singleNucleotideInsertionInPosition1() throws Exception {
        Variant variant = buildVariant(CHR_1, 1, "A", "TA", INSERTION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1, 1, ID, "A", "TA");
    }

    @Test
    public void singleNucleotideDeletionInPosition1() throws Exception {
        Variant variant = buildVariant(CHR_1, 1, "AT", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1, 2, ID, "AT", "T");
    }


    @Test
    public void severalNucleotidesInsertionInPosition1() throws Exception {
        Variant variant = buildVariant(CHR_1, 1, "A", "GGTA", INSERTION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1, 1, ID, "A", "GGTA");
    }

    @Test
    public void severalNucleotidesDeletionInPosition1() throws Exception {
        Variant variant = buildVariant(CHR_1, 1, "ATTG", "G", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);

        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        checkVariantContext(variantContext, CHR_1, 1, 4, ID, "ATTG", "G");
    }

    @Test
    public void twoStudiesSingleVariant() throws Exception {
        Variant variant = buildVariant(CHR_1, 1000, "T", "G", SNP_SEQUENCE_ONTOLOGY, STUDY_1, STUDY_2);

        // process variant
        VariantToVariantContextProcessor variantConverter = new VariantToVariantContextProcessor();
        VariantContext variantContext = variantConverter.process(variant);

        // check processed variant
        checkVariantContext(variantContext, CHR_1, 1000, 1000, ID, "T", "G");
        assertTrue(variantContext.getCommonInfo().hasAttribute(VARIANT_CLASS_KEY));
        assertTrue(variantContext.getCommonInfo().hasAttribute(STUDY_ID_KEY));
        String[] studies = ((String) variantContext.getCommonInfo().getAttribute(STUDY_ID_KEY)).split(",");
        assertEquals(Sets.newLinkedHashSet(STUDY_1, STUDY_2), Sets.newLinkedHashSet(studies));
    }

    private void checkVariantContext(VariantContext variantContext, String chromosome, int start, int end, String id,
                                     String ref, String alt) {
        assertEquals(chromosome, variantContext.getContig());
        assertEquals(start, variantContext.getStart());
        assertEquals(end, variantContext.getEnd());
        assertEquals(Allele.create(ref, true), variantContext.getReference());
        assertEquals(Collections.singletonList(Allele.create(alt, false)), variantContext.getAlternateAlleles());
        assertEquals(id, variantContext.getID());
        assertTrue(variantContext.getFilters().isEmpty());
        assertEquals(2, variantContext.getCommonInfo().getAttributes().size());
        assertEquals(0, variantContext.getSampleNames().size());
    }

}
