/*
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
 */
package uk.ac.ebi.eva.accession.clustering.batch.processors;

import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class VariantToSubmittedVariantEntityProcessorTest {

    private static final String ASSEMBLY_ACCESSION = "GCA_000000001.1";

    private static final String PROJECT_ACCESSION = "PROJECT_ACCESSION";

    private static final long SUBMITTED_VARIANT_ACCESSION = 1000L;

    private Function<ISubmittedVariant, String> hashingFunction;

    private VariantToSubmittedVariantEntityProcessor processor;

    @Before
    public void setUp() {
        processor = new VariantToSubmittedVariantEntityProcessor(ASSEMBLY_ACCESSION);
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Test
    public void process() {
        // given
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY_ACCESSION, 0, PROJECT_ACCESSION, "1", 1000L,
                                                                 "A", "T", null);
        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity expectedSubmittedVariantEntity = new SubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION,
                                                                                           hash, submittedVariant, 1);

        Variant variant = new Variant("1", 1000L, 1000L, "A", "T");
        variant.setMainId("ss" + SUBMITTED_VARIANT_ACCESSION);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute(VariantContextWriter.PROJECT_KEY, PROJECT_ACCESSION);
        variant.addSourceEntry(sourceEntry);

        // when
        SubmittedVariantEntity processedVariant = processor.process(variant);

        // then
        assertEquals(expectedSubmittedVariantEntity, processedVariant);
        assertEquals(expectedSubmittedVariantEntity.getAccession(), processedVariant.getAccession());
        assertEquals(expectedSubmittedVariantEntity.getHashedMessage(), processedVariant.getHashedMessage());
    }

    @Test
    public void ssIdMissing() {
        Variant variant = new Variant("1", 1000L, 1000L, "A", "T");
        assertThrows(IllegalArgumentException.class, () -> processor.process(variant));
    }

    @Test
    public void projectIsKept() {
        Variant variant = new Variant("1", 1000L, 1000L, "A", "T");
        variant.setMainId("ss" + SUBMITTED_VARIANT_ACCESSION);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute(VariantContextWriter.PROJECT_KEY, PROJECT_ACCESSION);
        variant.addSourceEntry(sourceEntry);

        SubmittedVariantEntity processed = processor.process(variant);

        assertEquals(PROJECT_ACCESSION, processed.getProjectAccession());
    }

    @Test
    public void rsIsKept() {
        Long rs = 30L;
        Variant variant = new Variant("1", 1000L, 1000L, "A", "T");
        variant.setMainId("ss" + SUBMITTED_VARIANT_ACCESSION);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute("RS", "rs" + rs.toString());
        sourceEntry.addAttribute(VariantContextWriter.PROJECT_KEY, PROJECT_ACCESSION);
        variant.addSourceEntry(sourceEntry);

        SubmittedVariantEntity processed = processor.process(variant);

        assertEquals(rs, processed.getClusteredVariantAccession());
    }

    @Test
    public void tooManySourceEntries() {
        Variant variant = new Variant("1", 1000L, 1000L, "A", "T");
        variant.setMainId("ss" + SUBMITTED_VARIANT_ACCESSION);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute(VariantContextWriter.PROJECT_KEY, PROJECT_ACCESSION);
        variant.addSourceEntry(sourceEntry);
        sourceEntry.setFileId("file2");
        variant.addSourceEntry(sourceEntry);

        assertThrows(IllegalArgumentException.class, () -> processor.process(variant));
    }
}
