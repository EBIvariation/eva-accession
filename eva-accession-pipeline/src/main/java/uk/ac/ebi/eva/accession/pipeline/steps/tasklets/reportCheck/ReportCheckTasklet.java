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
package uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compares the the original VCF and accession report VCF, and logs the differences.
 *
 * Terminology: The original VCF contains variants, and it will be loaded into the 'variantBuffer'. The accession
 * report VCF contains the accessioned variants, and it will be loaded into the 'accessionBuffer'.
 *
 * The high-level idea of the process is filling those 2 buffers, and when the same coordinates appears in both the
 * variantBuffer and the accessionBuffer, it means that the variant in those coordinates was correctly accessioned,
 * so it can be removed from both buffers. At the end, only variants without accessions and accessions without
 * variants should be left in the buffers after both files were completely read.
 *
 * The reason why the buffers are needed is that the accession report can be unordered. In order to avoid
 * having the original VCF completely loaded in the variantBuffer, a buffer size can be configured. However, to grant
 * correct behaviour in worst-ordering scenario, the accessionBuffer (for yet-unmatched accessions from the accession
 * report) has to be able to grow indefinitely.
 *
 * To perform self-checks, this tasklet provides the maximum size that the accessionBuffer during the execution, and
 * also provides the number of iterations needed.
 *
 */
public class ReportCheckTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(ReportCheckTasklet.class);

    private static final int DEFAULT_MAX_BUFFER_SIZE = 100000;

    private VcfReader vcfReader;

    private VcfReader reportReader;

    private long maxVariantBufferSize;

    private long maxAccessionBufferSize;

    private long iterations;

    public ReportCheckTasklet(VcfReader vcfReader, VcfReader reportReader) {
        this.vcfReader = vcfReader;
        this.reportReader = reportReader;
        this.maxVariantBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        this.maxAccessionBufferSize = 0;
        this.iterations = 0;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Set<Variant> variantBuffer = new HashSet<>();
        Set<Variant> accessionBuffer = new HashSet<>();
        while (true) {
            iterations++;

            readOriginalVcfUntilBufferIsFull(variantBuffer);
            removeMatchingVariants(variantBuffer, accessionBuffer);
            boolean isEveryAccessionLoaded = readAccessionsAndRemoveMatchingOnes(variantBuffer, accessionBuffer);

            maxAccessionBufferSize = Math.max(maxAccessionBufferSize, accessionBuffer.size());
            if (isEveryAccessionLoaded) {
                break;
            }
        }

        logger.debug("Max unmatched accessions held in memory: {}; iterations: {}", maxAccessionBufferSize, iterations);
        removeRemainingMatchingVariants(variantBuffer, accessionBuffer);

        logStatus(stepContribution, variantBuffer, accessionBuffer);

        return RepeatStatus.FINISHED;
    }

    /**
     * Fill the variantBuffer (from the original VCF) until the maximum buffer size is
     * reached, or until the file was completely read.
     * @param variantBuffer buffer that will be filled with the original VCF
     */
    private void readOriginalVcfUntilBufferIsFull(Set<Variant> variantBuffer) throws Exception {
        List<Variant> variantsRead;
        while (variantBuffer.size() < maxVariantBufferSize && (variantsRead = vcfReader.read()) != null) {
            variantBuffer.addAll(variantsRead);
        }
    }

    private void removeMatchingVariants(Set<Variant> variantBuffer, Set<Variant> accessionBuffer) {
        Set<Variant> matchedAccessions = new HashSet<>();
        for (Variant unmatchedAccession : accessionBuffer) {
            boolean removed = variantBuffer.remove(unmatchedAccession);
            if (removed) {
                matchedAccessions.add(unmatchedAccession);
            }
            if (variantBuffer.isEmpty()) {
                break;
            }
        }
        accessionBuffer.removeAll(matchedAccessions);
    }

    private boolean readAccessionsAndRemoveMatchingOnes(Set<Variant> variantBuffer, Set<Variant> accessionBuffer)
            throws Exception {
        Set<Variant> newUnmatchedAccessions = new HashSet<>();
        List<Variant> reportVariantsRead = null;
        newUnmatchedAccessions.clear();
        while (newUnmatchedAccessions.size() == 0 && (reportVariantsRead = reportReader.read()) != null) {
            for (Variant reportedVariant : reportVariantsRead) {
                boolean removed = variantBuffer.remove(reportedVariant);
                if (!removed) {
                    newUnmatchedAccessions.add(reportedVariant);
                }
            }
        }
        accessionBuffer.addAll(newUnmatchedAccessions);
        return reportVariantsRead == null;
    }

    private void removeRemainingMatchingVariants(Set<Variant> variantBuffer,
                                                 Set<Variant> accessionBuffer) throws Exception {
        removeMatchingVariants(variantBuffer, accessionBuffer);
        removeRemainingMatchingVariantsFromOriginalVcfFile(variantBuffer, accessionBuffer);
    }

    private void removeRemainingMatchingVariantsFromOriginalVcfFile(Set<Variant> variantBuffer,
                                                                    Set<Variant> accessionBuffer) throws Exception {
        List<Variant> variantsRead;
        while ((variantsRead = vcfReader.read()) != null) {
            for (Variant variantWithoutAccession : variantsRead) {
                boolean removed = accessionBuffer.remove(variantWithoutAccession);
                if (!removed) {
                    variantBuffer.add(variantWithoutAccession);
                }
            }
        }
    }

    private void logStatus(StepContribution stepContribution, Set<Variant> variantsWithoutAccession,
                           Set<Variant> unmatchedAccessions) {
        stepContribution.setExitStatus(ExitStatus.COMPLETED);

        if (!unmatchedAccessions.isEmpty()) {
            stepContribution.setExitStatus(ExitStatus.FAILED);
            logger.error("{} variants were found in the accession report that were not found in the original VCF.",
                         unmatchedAccessions.size());
            logger.info("These are the {} variants that were not found in the original VCF: {}",
                        unmatchedAccessions.size(), unmatchedAccessions);
        }

        if (!variantsWithoutAccession.isEmpty()) {
            stepContribution.setExitStatus(ExitStatus.FAILED);
            logger.error("{} variants were not found in the accession report.", variantsWithoutAccession.size());
            logger.info("These are the {} variants that were not found in the accession report: {}",
                        variantsWithoutAccession.size(), variantsWithoutAccession);
        }
    }

    public long getMaxVariantBufferSize() {
        return maxVariantBufferSize;
    }

    /**
     * See the terminology in the documentation of this class
     * @param maxVariantBufferSize max size of the buffer where the variants of the original VCF are loaded
     */
    public void setMaxVariantBufferSize(long maxVariantBufferSize) {
        this.maxVariantBufferSize = maxVariantBufferSize;
    }

    /**
     * To grant correct behaviour in worst-ordering scenario, the accessionBuffer (for yet-unmatched accessions from
     * the accession report) has to be able to grow indefinitely.
     * @return The maximum size of the accessionBuffer
     */
    public long getMaxAccessionBufferSize() {
        return maxAccessionBufferSize;
    }

    public long getIterations() {
        return iterations;
    }
}
