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
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.repeat.RepeatStatus;

import uk.ac.ebi.eva.accession.pipeline.policies.InvalidVariantSkipPolicy;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.HashSet;
import java.util.Set;

/**
 * Compares the the original VCF and accession report VCF, and logs the differences.
 *
 * Terminology: The original VCF contains variants, and it will be loaded into the 'variant buffer'. The accession
 * report VCF contains the accessioned variants, and it will be loaded into the 'accession buffer'.
 *
 * The high-level idea of the process is filling those 2 buffers, and when the same coordinates appears in both the
 * variant buffer and the accession buffer, it means that the variant in those coordinates was correctly accessioned,
 * so it can be removed from both buffers. At the end, only variants without accessions and accessions without
 * variants should be left in the buffers after both files were completely read.
 *
 * The reason why the buffers are needed is that the accession report can be unordered. In order to avoid
 * having the original VCF completely loaded in the variant buffer, a buffer size can be configured. However, to grant
 * correct behaviour in worst-ordering scenario, the accession buffer (for yet-unmatched accessions from the accession
 * report) has to be able to grow indefinitely.
 *
 * To perform self-checks, this tasklet provides the maximum size that the accession buffer during the execution, and
 * also provides the number of iterations needed.
 *
 */
public class ReportCheckTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(ReportCheckTasklet.class);

    private static final int DEFAULT_MAX_BUFFER_SIZE = 100000;

    private ItemStreamReader<Variant> inputReader;

    private ItemStreamReader<Variant> reportReader;

    private long maxVariantBufferSize;

    private long maxAccessionBufferSize;

    private long iterations;

    private SkipPolicy skipPolicy;

    public ReportCheckTasklet(ItemStreamReader<Variant> inputReader, ItemStreamReader<Variant> reportReader) {
        this.inputReader = inputReader;
        this.reportReader = reportReader;
        this.maxVariantBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        this.maxAccessionBufferSize = 0;
        this.iterations = 0;
        this.skipPolicy = new InvalidVariantSkipPolicy();
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Set<Variant> variantBuffer = new HashSet<>();
        Set<Variant> accessionBuffer = new HashSet<>();
        inputReader.open(new ExecutionContext());
        reportReader.open(new ExecutionContext());
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
        Variant variantRead;
        while (variantBuffer.size() < maxVariantBufferSize && (variantRead = readVcfIgnoringNonVariants()) != null) {
            variantBuffer.add(variantRead);
        }
    }

    private Variant readVcfIgnoringNonVariants() throws Exception {
        Variant variant = null;
        boolean readVariantOrEof = false;
        while (!readVariantOrEof) {
            try {
                variant = inputReader.read();
                readVariantOrEof = true;
            } catch (Exception exception) {
                if (skipPolicy.shouldSkip(exception, 0)) {
                    // this was a non-variant, we must read the next line
                } else {
                    throw exception;
                }
            }
        }
        return variant;
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
        Variant reportedVariant = null;
        while (newUnmatchedAccessions.size() == 0 && (reportedVariant = reportReader.read()) != null) {
            boolean removed = variantBuffer.remove(reportedVariant);
            if (!removed) {
                newUnmatchedAccessions.add(reportedVariant);
            }
        }
        accessionBuffer.addAll(newUnmatchedAccessions);
        return reportedVariant == null;
    }

    private void removeRemainingMatchingVariants(Set<Variant> variantBuffer,
                                                 Set<Variant> accessionBuffer) throws Exception {
        removeMatchingVariants(variantBuffer, accessionBuffer);
        removeRemainingMatchingVariantsFromOriginalVcfFile(variantBuffer, accessionBuffer);
    }

    private void removeRemainingMatchingVariantsFromOriginalVcfFile(Set<Variant> variantBuffer,
                                                                    Set<Variant> accessionBuffer) throws Exception {
        Variant variantWithoutAccession;
        while ((variantWithoutAccession = readVcfIgnoringNonVariants()) != null) {
            boolean removed = accessionBuffer.remove(variantWithoutAccession);
            if (!removed) {
                variantBuffer.add(variantWithoutAccession);
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
     * To guarantee correct behaviour in worst-ordering scenario, the accession buffer (for yet-unmatched accessions
     * from the accession report) has to be able to grow indefinitely.
     * @return The maximum size of the accessionBuffer
     */
    public long getMaxAccessionBufferSize() {
        return maxAccessionBufferSize;
    }

    public long getIterations() {
        return iterations;
    }
}
