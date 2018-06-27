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
 * <p>
 * Terminology: The original VCF contains variants, and it will be loaded into the 'variant buffer'. The accession
 * report VCF contains the accessioned variants, and it will be loaded into the 'accession buffer'.
 * <p>
 * The high-level idea of the process is filling those 2 buffers, and when the same coordinates appears in both the
 * variant buffer and the accession buffer, it means that the variant in those coordinates was correctly accessioned,
 * so it can be removed from both buffers. At the end, only variants without accessions and accessions without
 * variants should be left in the buffers after both files were completely read.
 * <p>
 * The reason why the buffers are needed is that the accession report can be unordered. In order to avoid
 * having the VCFs completely loaded in memory, a buffer size can be configured for both buffers. However, to guarantee
 * correct behaviour in worst-ordering scenario, the buffers need to be able to grow indefinitely.
 * <p>
 * To perform some self-checks, this tasklet provides the maximum size of the buffers during the execution, and
 * also provides the number of iterations needed.
 */
public class ReportCheckTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(ReportCheckTasklet.class);

    private static final int BUFFER_SIZE_INCREASE_FACTOR = 2;

    private ItemStreamReader<Variant> inputReader;

    private ItemStreamReader<Variant> reportReader;

    private long maxBufferSize;

    private long initialBufferSize;

    private long iterations;

    private SkipPolicy skipPolicy;

    private long duplicatedVariantsInInputVcf;

    private long duplicatedVariantsInReportVcf;

    private long skippedVariantsInInputVcf;

    private long skippedVariantsInReportVcf;

    public ReportCheckTasklet(ItemStreamReader<Variant> inputReader, ItemStreamReader<Variant> reportReader,
                              long initialBufferSize) {
        this.inputReader = inputReader;
        this.reportReader = reportReader;
        this.initialBufferSize = initialBufferSize;
        this.maxBufferSize = 0;
        this.iterations = 0;
        this.skipPolicy = new InvalidVariantSkipPolicy();
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Set<Variant> variantBuffer = new HashSet<>();
        Set<Variant> accessionBuffer = new HashSet<>();
        inputReader.open(new ExecutionContext());
        reportReader.open(new ExecutionContext());
        boolean readsPendingInputFile = true;
        boolean readsPendingReportFile = true;
        while (readsPendingInputFile || readsPendingReportFile) {
            iterations++;

            readsPendingInputFile = fillBuffer(inputReader, variantBuffer);
            readsPendingReportFile = fillBuffer(reportReader, accessionBuffer);

            int numRemoved = removeMatchingVariants(variantBuffer, accessionBuffer);
            if (numRemoved == 0) {
                increaseBufferSizeToAvoidLock();
            }
        }

        logStatus(stepContribution, variantBuffer, accessionBuffer);

        return RepeatStatus.FINISHED;
    }

    /**
     * @return true if the reader is pending some reads, false otherwise (i.e. return false if there are no more
     * reads available)
     */
    private boolean fillBuffer(ItemStreamReader<Variant> reader, Set<Variant> buffer) throws Exception {
        Variant variantRead = null;
        boolean bufferWasFull = buffer.size() >= initialBufferSize;

        while (buffer.size() < initialBufferSize && (variantRead = readVcfIgnoringNonVariants(reader)) != null) {
            buffer.add(variantRead);
        }
        maxBufferSize = Math.max(maxBufferSize, buffer.size());
        boolean moreReadsPending = bufferWasFull || variantRead != null;
        return moreReadsPending;
    }

    private Variant readVcfIgnoringNonVariants(ItemStreamReader<Variant> inputReader) throws Exception {
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

    private int removeMatchingVariants(Set<Variant> variantBuffer, Set<Variant> accessionBuffer) {
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
        return matchedAccessions.size();
    }

    /**
     * TODO: decide if we allow to grow indefinitely or put a hard cap
     * A hard cap would mean we stop the process if there are more unmatched variants than the buffer size or the
     * report is heavily unordered.
     * Alternatively, we can allow configuring which behaviour to use?
     */
    private void increaseBufferSizeToAvoidLock() {
        initialBufferSize *= BUFFER_SIZE_INCREASE_FACTOR;
    }

    private void logStatus(StepContribution stepContribution, Set<Variant> variantsWithoutAccession,
                           Set<Variant> unmatchedAccessions) {
        logger.debug("Max unmatched accessions held in memory: {}; iterations: {}", maxBufferSize, iterations);

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

    /**
     * @param initialBufferSize initial size of the buffers. This will grow exponentially as needed.
     */
    public void setInitialBufferSize(long initialBufferSize) {
        this.initialBufferSize = initialBufferSize;
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public long getIterations() {
        return iterations;
    }

    public long getDuplicatedVariantsInInputVcf() {
        return duplicatedVariantsInInputVcf;
    }

    public long getDuplicatedVariantsInReportVcf() {
        return duplicatedVariantsInReportVcf;
    }

    public long getSkippedVariantsInInputVcf() {
        return skippedVariantsInInputVcf;
    }

    public long getSkippedVariantsInReportVcf() {
        return skippedVariantsInReportVcf;
    }
}
