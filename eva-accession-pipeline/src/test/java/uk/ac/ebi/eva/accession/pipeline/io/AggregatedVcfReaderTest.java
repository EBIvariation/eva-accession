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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.MetaDataInstanceFactory;

import uk.ac.ebi.eva.accession.pipeline.utils.FileUtils;
import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * {@link AggregatedVcfReader}
 * input: a Vcf file
 * output: a list of variants each time its `.read()` is called
 */
public class AggregatedVcfReaderTest {

    private static final String FILE_ID = "5";

    private static final String STUDY_ID = "7";

    private static final String INPUT_FILE_PATH = "/input-files/vcf/aggregated.vcf.gz";

    private static final String INPUT_FILE_PATH_EXAC = "/input-files/vcf/aggregated.exac.vcf.gz";

    private static final String INPUT_FILE_PATH_EVS = "/input-files/vcf/aggregated.evs.vcf.gz";

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Test
    public void shouldReadAllLines() throws Exception {
        shouldReadAllLinesHelper(Aggregation.BASIC, INPUT_FILE_PATH);
    }

    @Test
    public void shouldReadAllLinesInExac() throws Exception {
        shouldReadAllLinesHelper(Aggregation.EXAC, INPUT_FILE_PATH_EXAC);
    }

    @Test
    public void shouldReadAllLinesInEvs() throws Exception {
        shouldReadAllLinesHelper(Aggregation.EVS, INPUT_FILE_PATH_EVS);
    }

    private void shouldReadAllLinesHelper(Aggregation aggregationType,
                                          String inputFilePath) throws Exception {

        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // input vcf
        File input = FileUtils.getResourceFile(inputFilePath);

        AggregatedVcfReader vcfReader = new AggregatedVcfReader(FILE_ID, STUDY_ID, aggregationType, null,
                                                                input);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, vcfReader);
    }

    @Test
    public void testUncompressedVcf() throws Exception {
        ExecutionContext executionContext = MetaDataInstanceFactory.createStepExecution().getExecutionContext();

        // uncompress the input VCF into a temporal file
        File input = FileUtils.getResourceFile(INPUT_FILE_PATH);
        File tempFile = temporaryFolderRule.newFile();
        FileUtils.uncompress(input.getAbsolutePath(), tempFile);

        AggregatedVcfReader vcfReader = new AggregatedVcfReader(FILE_ID, STUDY_ID, Aggregation.BASIC,
                                                                null, tempFile);
        vcfReader.setSaveState(false);
        vcfReader.open(executionContext);

        consumeReader(input, vcfReader);
    }

    private void consumeReader(File inputFile, AggregatedVcfReader vcfReader) throws Exception {
        List<Variant> variants;
        int count = 0;

        // consume the reader and check that the variants and the VariantSource have meaningful data
        while ((variants = vcfReader.read()) != null) {
            assertTrue(variants.size() > 0);
            assertTrue(variants.get(0).getSourceEntries().size() > 0);
            VariantSourceEntry sourceEntry = variants.get(0).getSourceEntries().iterator().next();
            assertTrue(sourceEntry.getSamplesData().isEmpty()); // by definition, aggregated VCFs don't have sample data
            assertFalse(sourceEntry.getCohortStats(VariantSourceEntry.DEFAULT_COHORT).getGenotypesCount().isEmpty());

            count++;
        }

        // AggregatedVcfReader should get all the lines from the file
        long expectedCount = FileUtils.countNonCommentLines(new GZIPInputStream(new FileInputStream(inputFile)));
        assertEquals(expectedCount, count);
    }
}
