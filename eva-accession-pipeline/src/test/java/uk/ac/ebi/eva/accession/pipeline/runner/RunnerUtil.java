package uk.ac.ebi.eva.accession.pipeline.runner;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class RunnerUtil {
    public static void injectErrorIntoTempVcf(String modifiedVcfContent, File tempVcfInputFile) throws Exception {
        // Inject error in the VCF file to cause processing to stop at variant#9
        writeToTempVCFFile(modifiedVcfContent, tempVcfInputFile);
    }

    public static void remediateTempVcfError(String originalVcfContent, File tempVcfInputFile) throws Exception {
        writeToTempVCFFile(originalVcfContent, tempVcfInputFile);
    }

    public static void useOriginalVcfFile(InputParameters inputParameters, String originalVcfInputFilePath, VcfReader vcfReader) throws Exception {
        inputParameters.setVcf(originalVcfInputFilePath);
        vcfReader.setResource(FileUtils.getResource(new File(originalVcfInputFilePath)));
    }

    public static void useTempVcfFile(InputParameters inputParameters, File tempVcfInputFile, VcfReader vcfReader) throws Exception {
        // The following does not actually change the wiring of the vcfReader since the wiring happens before the tests
        // This setVcf is only to facilitate identifying jobs in the job repo by parameter
        // (those that use original vs temp VCF)
        inputParameters.setVcf(tempVcfInputFile.getAbsolutePath());
        /*
         * Change the auto-wired VCF for VCFReader at runtime
         * Rationale:
         *  1) Why not use two test configurations, one for a VCF that fails validation and another for a VCF
         *  that won't and test resumption?
         *     Beginning Spring Boot 2, job resumption can only happen when input parameters to the restarted job
         *     is the same as the failed job.
         *     Therefore, a test to check resumption cannot have two different config files with different
         *     parameters.vcf.
         *     This test therefore creates a dynamic VCF and injects errors at runtime to the VCF thus preserving
         *     the VCF parameter but changing the VCF content.
         *  2) Why not artificially inject a VcfReader exception?
         *     This will preclude us from verifying job resumption from a precise line in the VCF.
         */
        vcfReader.setResource(FileUtils.getResource(tempVcfInputFile));
    }

    public static void writeToTempVCFFile(String modifiedVCFContent, File tempVcfInputFile) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(tempVcfInputFile.getAbsolutePath());
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(modifiedVCFContent.getBytes(StandardCharsets.UTF_8));
        gzipOutputStream.close();
    }

    public static String getOriginalVcfContent(String inputVcfPath) throws Exception {
        StringBuilder originalVCFContent = new StringBuilder();

        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inputVcfPath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream));

        String read;
        while ((read = reader.readLine()) != null) {
            originalVCFContent.append(read).append(System.lineSeparator());
        }
        return originalVCFContent.toString();
    }

    public static void deleteTemporaryContigAndVariantFiles(InputParameters inputParameters, Path tempVcfOutputDir) throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + ".variants"));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + ".contigs"));
        Files.deleteIfExists(Paths.get(tempVcfOutputDir + "/accession-output.vcf.variants"));
        Files.deleteIfExists(Paths.get(tempVcfOutputDir + "/accession-output.vcf.contigs"));
    }
}
