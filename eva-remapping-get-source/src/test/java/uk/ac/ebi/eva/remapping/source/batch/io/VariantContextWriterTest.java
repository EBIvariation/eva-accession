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
package uk.ac.ebi.eva.remapping.source.batch.io;

import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.remapping.source.batch.processors.SubmittedVariantToVariantContextProcessor;
import uk.ac.ebi.eva.remapping.source.parameters.ReportPathResolver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VariantContextWriterTest {

    private static final String REFERENCE_ASSEMBLY = "GCA_00000XXX.X";

    public static final int COLUMNS_IN_VCF_WITHOUT_SAMPLES = 8;

    public static final int VCF_CHROMOSOME_COLUMN = 0;

    public static final int VCF_POSITION_COLUMN = 1;

    public static final int VCF_REFERENCE_ALLELE_COLUMN = 3;

    public static final int VCF_ALTERNATE_ALLELE_COLUMN = 4;

    public static final int VCF_INFO_COLUMN = 7;

    public static final String PROJECT_ACCESSION = "project1";

    public static final int TAXONOMY_ACCESSION = 9606;

    public static final String CREATED_DATE = "2021-06-22T10:10:10.100";

    public static final String SS_HASH = "hash1";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void basicWrite() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", 123L));
    }

    private File assertWriteVcf(File outputFolder, SubmittedVariantEntity... variants) throws Exception {
        Path reportPath = ReportPathResolver.getEvaReportPath(outputFolder.getAbsolutePath(), REFERENCE_ASSEMBLY);
        VariantContextWriter writer = new VariantContextWriter(reportPath, REFERENCE_ASSEMBLY);
        writer.open(null);

        SubmittedVariantToVariantContextProcessor variantToVariantContextProcessor =
                new SubmittedVariantToVariantContextProcessor();

        List<VariantContext> variantContexts =
                Stream.of(variants).map(variantToVariantContextProcessor::process).collect(Collectors.toList());
        writer.write(variantContexts);

        writer.close();

        File output = writer.getOutput();
        assertTrue(output.exists());

        return output;
    }

    private SubmittedVariantEntity buildVariant(String chr, long start, String ref, String alt, Long rs) {
        return buildVariant(chr, start, ref, alt, rs, PROJECT_ACCESSION, TAXONOMY_ACCESSION);
    }

    private SubmittedVariantEntity buildVariant(String chr, long start, String ref, String alt, Long rs,
                                                String project, int taxonomy) {
        return buildVariant(chr, start, ref, alt, rs, null, project, taxonomy);
    }

    private SubmittedVariantEntity buildVariant(String chr, long start, String ref, String alt, Long rs,
                                                Long backpropRs, String project, int taxonomy) {
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(1L, SS_HASH, REFERENCE_ASSEMBLY,
                                                                                   taxonomy, project, chr, start, ref,
                                                                                   alt, rs, false, false, false, false,
                                                                                   1);
        if (backpropRs != null) {
            submittedVariantEntity.setBackPropagatedVariantAccession(backpropRs);
        }
        submittedVariantEntity.setCreatedDate(LocalDateTime.parse(CREATED_DATE));
        return submittedVariantEntity;
    }

    @Test
    public void writeBasicFields() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        String chromosome = "1";
        long start = 1000;
        String reference = "C";
        String alternate = "A";
        File output = assertWriteVcf(outputFolder, buildVariant(chromosome, start, reference, alternate, null));

        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertEquals(chromosome, columns[VCF_CHROMOSOME_COLUMN]);
            assertEquals(start, Long.parseLong(columns[VCF_POSITION_COLUMN]));
            assertEquals(reference, columns[VCF_REFERENCE_ALLELE_COLUMN]);
            assertEquals(alternate, columns[VCF_ALTERNATE_ALLELE_COLUMN]);
        });
        assertEquals(1, variantCount);
    }

    private long forEachVcfDataLine(File output, Consumer<String[]> processVcfDataLine) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(output));
        String line;
        long variants = 0;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                continue;
            }
            variants++;
            processVcfDataLine.accept(line.split("\t"));
        }
        return variants;
    }

    @Test
    public void writeRsId() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        long rsId = 123L;
        File output = assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", rsId));

        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertInfo(rsId, columns[VCF_INFO_COLUMN]);
        });
        assertEquals(1, variantCount);
    }

    private void assertInfo(Long rsId, String infoColumn) {
        assertInfo(infoColumn, SS_HASH, rsId, null, PROJECT_ACCESSION, TAXONOMY_ACCESSION, CREATED_DATE);
    }

    private void assertInfo(String infoColumn, String expectedSSHash, Long expectedRsId, Long expectedBackpropRs,
                            String expectedProject, int expectedTaxonomy, String expectedCreatedDate) {
        String[] infos = infoColumn.split(";");

        int expectedLength = 4;
        if (expectedRsId != null) {
            expectedLength++;
        }
        if (expectedBackpropRs != null) {
            expectedLength++;
        }
        assertEquals(expectedLength, infos.length);

        for (String info : infos) {
            if (info.startsWith("SS_HASH=")) {
                assertEquals("SS_HASH=" + expectedSSHash, info);
            } else if (info.startsWith("RS=")) {
                assertEquals("RS=rs" + expectedRsId, info);
            } else if (info.startsWith("BACKPROP_RS=")) {
                assertEquals("BACKPROP_RS=rs" + expectedBackpropRs, info);
            } else if(info.startsWith("PROJECT=")) {
                assertEquals("PROJECT=" + expectedProject, info);
            } else if(info.startsWith("TAX=")) {
                assertEquals("TAX=" + expectedTaxonomy, info);
            } else if(info.startsWith("CREATED=")) {
                assertEquals("CREATED=" + expectedCreatedDate, info);
            }
        }
    }

    @Test
    public void writeProjectAndTaxonomy() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        String project2 = "project2";
        int taxonomy2 = 1000;
        File output = assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", null, project2, taxonomy2));

        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertInfo(columns[VCF_INFO_COLUMN], SS_HASH, null, null, project2, taxonomy2, CREATED_DATE);
        });
        assertEquals(1, variantCount);
    }

    @Test
    public void writeProjectAndTaxonomyAndRs() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        String project2 = "project2";
        int taxonomy2 = 1000;
        long rsId = 123L;
        File output = assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", rsId, project2, taxonomy2));

        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertInfo(columns[VCF_INFO_COLUMN], SS_HASH, rsId, null, project2, taxonomy2, CREATED_DATE);
        });
        assertEquals(1, variantCount);
    }


    @Test
    public void writeSeveralVariants() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        String project2 = "project2";
        int taxonomy2 = 1000;
        long rsId = 123L;
        long backpropRs = 456L;
        File output = assertWriteVcf(outputFolder,
                                     buildVariant("1", 1000, "C", "A", rsId, backpropRs, project2, taxonomy2),
                                     buildVariant("2", 1000, "C", "A", rsId, backpropRs, project2, taxonomy2),
                                     buildVariant("3", 1000, "C", "A", rsId, backpropRs, project2, taxonomy2),
                                     buildVariant("4", 1000, "C", "A", rsId, backpropRs, project2, taxonomy2));

        final Long[] expectedChr = {1L};
        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertEquals(expectedChr[0].toString(), columns[VCF_CHROMOSOME_COLUMN]);
            assertInfo(columns[VCF_INFO_COLUMN], SS_HASH, rsId, backpropRs, project2, taxonomy2, CREATED_DATE);
            expectedChr[0]++;
        });
        assertEquals(4, variantCount);
    }

    @Test
    public void writeProjectWithSpecialCharacters() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        String specialProject = "a%weird=project;with,special characters";
        File output = assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", null, specialProject,
                                                                TAXONOMY_ACCESSION));

        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertInfo(columns[VCF_INFO_COLUMN], SS_HASH, null, null, "a%25weird%3Dproject%3Bwith%2Cspecial characters", TAXONOMY_ACCESSION,
                    CREATED_DATE);
        });
        assertEquals(1, variantCount);
    }
}