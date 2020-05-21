package uk.ac.ebi.eva.accession.remapping.batch.io;

import htsjdk.variant.variantcontext.VariantContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.remapping.batch.processors.SubmittedVariantToVariantContextProcessor;
import uk.ac.ebi.eva.accession.remapping.parameters.ReportPathResolver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
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

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void basicWrite() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        File output = assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", 123L));
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
        String[] infos = infoColumn.split(";");
        if (rsId != null) {
            assertEquals(2, infos.length);
            String project, rs;
            if (infos[0].startsWith("RS=")) {
                project = infos[1];
                rs = infos[0];
            } else {
                project = infos[0];
                rs = infos[1];
            }
            assertEquals("RS=rs" + rsId, rs);
            assertTrue(project.startsWith("PROJECT="));
        } else {
            assertEquals(1, infos.length);
            assertTrue(infos[0].startsWith("PROJECT="));
        }
    }

    @Test
    public void writeEmptyInfoIfMissingRsId() throws Exception {
        File outputFolder = temporaryFolder.newFolder();
        File output = assertWriteVcf(outputFolder, buildVariant("1", 1000, "C", "A", null));

        long variantCount = forEachVcfDataLine(output, (String[] columns) -> {
            assertEquals(COLUMNS_IN_VCF_WITHOUT_SAMPLES, columns.length);
            assertInfo(null, columns[VCF_INFO_COLUMN]);
        });
        assertEquals(1, variantCount);
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
        return new SubmittedVariantEntity(1L, "hash1", REFERENCE_ASSEMBLY, 9606, "project1", chr, start, ref, alt, rs,
                                          false, false, false, false, 1);
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
}