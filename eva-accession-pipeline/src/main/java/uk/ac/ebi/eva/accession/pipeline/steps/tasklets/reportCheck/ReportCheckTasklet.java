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

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
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

    private BufferHelper inputBufferHelper;

    private BufferHelper reportBufferHelper;

    private long maxBufferSize;

    private long initialBufferSize;

    private long iterations;

    private SkipPolicy skipPolicy;

    private ContigMapping contigMapping;

    public ReportCheckTasklet(ItemStreamReader<Variant> inputReader, ItemStreamReader<Variant> reportReader,
                              long initialBufferSize, ContigMapping contigMapping) {
        this.inputBufferHelper = new BufferHelper(inputReader);
        this.reportBufferHelper = new BufferHelper(reportReader);
        this.initialBufferSize = initialBufferSize;
        this.contigMapping = contigMapping;
        this.maxBufferSize = 0;
        this.iterations = 0;
        this.skipPolicy = new InvalidVariantSkipPolicy();
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        inputBufferHelper.getReader().open(new ExecutionContext());
        reportBufferHelper.getReader().open(new ExecutionContext());
        boolean readsPendingInputFile = true;
        boolean readsPendingReportFile = true;
        while (readsPendingInputFile || readsPendingReportFile) {
            iterations++;

            readsPendingInputFile = fillBuffer(inputBufferHelper);
            readsPendingReportFile = fillBuffer(reportBufferHelper);

            int numRemoved = removeMatchingVariants(inputBufferHelper.getBuffer(), reportBufferHelper.getBuffer());
            if (numRemoved == 0) {
                increaseBufferSizeToAvoidLock();
            }
        }

        logStatus(stepContribution);

        return RepeatStatus.FINISHED;
    }

    /**
     * @return true if the reader is pending some reads, false otherwise (i.e. return false if there are no more
     * reads available)
     */
    private boolean fillBuffer(BufferHelper bufferHelper) throws Exception {
        Variant variantRead = null;
        Set<Variant> buffer = bufferHelper.getBuffer();
        boolean bufferWasFull = buffer.size() >= initialBufferSize;

        while (buffer.size() < initialBufferSize && (variantRead = readVcfIgnoringNonVariants(bufferHelper)) != null) {
            if (!buffer.add(variantRead)) {
                bufferHelper.setDuplicatedVariants(bufferHelper.getDuplicatedVariants() + 1);
            }
        }
        maxBufferSize = Math.max(maxBufferSize, buffer.size());
        boolean moreReadsPending = bufferWasFull || variantRead != null;
        return moreReadsPending;
    }

    private Variant readVcfIgnoringNonVariants(BufferHelper bufferHelper) throws Exception {
        Variant variant = null;
        boolean readVariantOrEof = false;
        while (!readVariantOrEof) {
            try {
                variant = bufferHelper.getReader().read();
                readVariantOrEof = true;
            } catch (Exception exception) {
                if (skipPolicy.shouldSkip(exception, 0)) {
                    // this was likely a non-variant, we must read the next line
                    bufferHelper.setSkippedVariants(bufferHelper.getSkippedVariants() + 1);
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

    private void logStatus(StepContribution stepContribution) {
        logger.debug("Max unmatched accessions held in memory: {}; iterations: {}", maxBufferSize, iterations);

        if (inputBufferHelper.getSkippedVariants() > 0) {
            logger.warn("{} lines in the original VCF were skipped. The most likely reason is that they were " +
                                "non-variants, but a high number could be symptom of a problem.",
                        inputBufferHelper.getSkippedVariants());
        }

        if (reportBufferHelper.getSkippedVariants() > 0) {
            logger.error("{} variants in the accession report were skipped. This is very likely a bug because the " +
                                 "report should not contain non-variants nor malformed lines.",
                         reportBufferHelper.getSkippedVariants());
        }

        if (inputBufferHelper.getDuplicatedVariants() > 0) {
            logger.warn("{} duplicated variants were found in the original VCF. This means the report should have " +
                                "less variants than the original VCF, as each set of duplicates got only one accession.",
                        inputBufferHelper.getDuplicatedVariants());
        }
        if (reportBufferHelper.getDuplicatedVariants() > 0) {
            logger.warn("{} duplicated variants were found in the accession report. This means that in the original " +
                                "VCF there were duplicates and they got different accessions, and now there are " +
                                "redundant accessions that should be eventually deprecated. These redundant " +
                                "accessions will be avoided when aligning against the reference sequence.",
                        reportBufferHelper.getDuplicatedVariants());
        }

        stepContribution.setExitStatus(ExitStatus.COMPLETED);
        if (!reportBufferHelper.getBuffer().isEmpty()) {
            stepContribution.setExitStatus(ExitStatus.FAILED);
            logger.error("{} variants were found in the accession report that were not found in the original VCF.",
                         reportBufferHelper.getBuffer().size());
            logger.info("These are the {} variants that were not found in the original VCF: {}",
                        reportBufferHelper.getBuffer().size(), reportBufferHelper.getBuffer());
        }

        if (!inputBufferHelper.getBuffer().isEmpty()) {
            stepContribution.setExitStatus(ExitStatus.FAILED);
            logger.error("{} variants were not found in the accession report. Given that {} of those are duplicates, " +
                                 "only {} - {} = {} unaccessioned variants need to be checked.",
                         inputBufferHelper.getBuffer().size(), reportBufferHelper.getDuplicatedVariants(),
                         inputBufferHelper.getBuffer().size(), reportBufferHelper.getDuplicatedVariants(),
                         inputBufferHelper.getBuffer().size() - reportBufferHelper.getDuplicatedVariants());
            logger.info("These are the {} variants that were not found in the accession report: {}",
                        inputBufferHelper.getBuffer().size(), inputBufferHelper.getBuffer());
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
        return inputBufferHelper.getDuplicatedVariants();
    }

    public long getDuplicatedVariantsInReportVcf() {
        return reportBufferHelper.getDuplicatedVariants();
    }

    public long getSkippedVariantsInInputVcf() {
        return inputBufferHelper.getSkippedVariants();
    }

    public long getSkippedVariantsInReportVcf() {
        return reportBufferHelper.getSkippedVariants();
    }

    public long getUnmatchedVariantsInInputVcf() {
        return inputBufferHelper.getBuffer().size();
    }

    public long getUnmatchedVariantsInReportVcf() {
        return reportBufferHelper.getBuffer().size();
    }

    private class BufferHelper {

        private ItemStreamReader<Variant> reader;

        private Set<Variant> buffer;

        private Long duplicatedVariants;

        private Long skippedVariants;

        BufferHelper(ItemStreamReader<Variant> reportReader) {
            this.reader = reportReader;
            this.buffer = new HashSet<>();
            this.duplicatedVariants = 0L;
            this.skippedVariants = 0L;
        }

        public ItemStreamReader<Variant> getReader() {
            return reader;
        }

        public void setReader(ItemStreamReader<Variant> reader) {
            this.reader = reader;
        }

        public Set<Variant> getBuffer() {
            return buffer;
        }

        public void setBuffer(Set<Variant> buffer) {
            this.buffer = buffer;
        }

        public Long getDuplicatedVariants() {
            return duplicatedVariants;
        }

        public void setDuplicatedVariants(Long duplicatedVariants) {
            this.duplicatedVariants = duplicatedVariants;
        }

        public Long getSkippedVariants() {
            return skippedVariants;
        }

        public void setSkippedVariants(Long skippedVariants) {
            this.skippedVariants = skippedVariants;
        }
    }
}
