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
package uk.ac.ebi.eva.accession.pipeline.steps.tasklets;

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

public class ReportCheckTasklet implements Tasklet {

    private static final Logger logger = LoggerFactory.getLogger(ReportCheckTasklet.class);

    private static final int DEFAULT_MAX_BUFFER_SIZE = 10000;

    private VcfReader vcfReader;

    private VcfReader reportReader;

    private long maxBufferSize;

    public ReportCheckTasklet(VcfReader vcfReader, VcfReader reportReader) {
        this.vcfReader = vcfReader;
        this.reportReader = reportReader;
        this.maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        Set<Variant> variantsWithoutAccessionYet = new HashSet<>();
        Set<Variant> unmatchedAccessions = new HashSet<>();
        Set<Variant> unmatchedAccessionsInChunk = new HashSet<>();
        List<Variant> variantsRead;
        while (true) {
            while (variantsWithoutAccessionYet.size() < maxBufferSize && (variantsRead = vcfReader.read()) != null) {
                variantsWithoutAccessionYet.addAll(variantsRead);
            }

            List<Variant> reportVariantsRead = null;
            while (unmatchedAccessionsInChunk.size() == 0 && (reportVariantsRead = reportReader.read()) != null) {
                for (Variant reportedVariant : reportVariantsRead) {
                    boolean removed = variantsWithoutAccessionYet.remove(reportedVariant);
                    if (!removed) {
                        unmatchedAccessionsInChunk.add(reportedVariant);
                    }
                }
            }
            unmatchedAccessions.addAll(unmatchedAccessionsInChunk);
            unmatchedAccessionsInChunk.clear();
            if (reportVariantsRead == null) {
                break;
            }
            logger.debug("unmatched accessions size: {}", unmatchedAccessions.size());
        }

        logger.debug("max unmatched accessions size: {}", unmatchedAccessions.size());
        Set<Variant> variantsWithoutAccession = new HashSet<>();
        for (Variant variantWithoutAccession : variantsWithoutAccessionYet) {
            boolean removed = unmatchedAccessions.remove(variantWithoutAccession);
            if (!removed) {
                variantsWithoutAccession.add(variantWithoutAccession);
            }
        }
        while ((variantsRead = vcfReader.read()) != null) {
            for (Variant variantWithoutAccession : variantsRead) {
                boolean removed = unmatchedAccessions.remove(variantWithoutAccession);
                if (!removed) {
                    variantsWithoutAccession.add(variantWithoutAccession);
                }
            }
        }

        logStatus(stepContribution, variantsWithoutAccession, unmatchedAccessions);

        return RepeatStatus.FINISHED;
    }

    private void logStatus(StepContribution stepContribution, Set<Variant> variantsWithoutAccession,
                           Set<Variant> mismatchingAccessions) {
        stepContribution.setExitStatus(ExitStatus.COMPLETED);

        if (!mismatchingAccessions.isEmpty()) {
            stepContribution.setExitStatus(ExitStatus.FAILED);
            logger.error("{} variants were found in the accession report that were not found in the original report. " +
                                 "This might be caused by alignment issues or a bug in the code.",
                         mismatchingAccessions.size());
            logger.info("These are the {} variants that were not found in the original report: {}",
                        mismatchingAccessions.size(), mismatchingAccessions);
        }

        if (!variantsWithoutAccession.isEmpty()) {
            stepContribution.setExitStatus(ExitStatus.FAILED);
            logger.error("{} variants were not found in the accession report.", variantsWithoutAccession.size());
            logger.info("These are the {} variants that were not found in the accession report: {}",
                        variantsWithoutAccession.size(), variantsWithoutAccession);
        }
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public void setMaxBufferSize(long maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }
}
