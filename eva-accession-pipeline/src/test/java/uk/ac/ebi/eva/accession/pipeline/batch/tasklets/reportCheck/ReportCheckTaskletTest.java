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
package uk.ac.ebi.eva.accession.pipeline.batch.tasklets.reportCheck;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.batch.io.AccessionedVcfLineMapper;
import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfReader;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.QC_SUBSNP_ACCESSION_STEP;

public class ReportCheckTaskletTest {

    private static final long JOB_ID = 0L;

    private ContigMapping contigMapping = new ContigMapping(
            Collections.singletonList(new ContigSynonyms("20", "assembled-molecule", "20", "20", "20", "20", true)));

    @Test
    public void correctReport() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.report.vcf.gz").toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInInputVcf());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInReportVcf());
    }

    private ReportCheckTasklet getReportCheckTasklet(URI vcfUri, URI reportUri) throws IOException {
        return getReportCheckTasklet(vcfUri, reportUri, 1000);
    }

    private ReportCheckTasklet getReportCheckTasklet(URI vcfUri, URI reportUri, int initialBufferSize) throws IOException {
        File vcfFile = new File(vcfUri);
        AggregatedVcfReader vcfReader = new AggregatedVcfReader("fileId", "studyId", Aggregation.BASIC, null, vcfFile);
        UnwindingItemStreamReader<Variant> unwindingVcfReader = new UnwindingItemStreamReader<>(vcfReader);

        File reportFile = new File(reportUri);
        VcfReader reportReader = new VcfReader(new AccessionedVcfLineMapper(), reportFile);
        UnwindingItemStreamReader<Variant> unwindingReportReader = new UnwindingItemStreamReader<>(reportReader);

        return new ReportCheckTasklet(unwindingVcfReader, unwindingReportReader, initialBufferSize, contigMapping);
    }

    @Test
    public void variantMissingInReport() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.incomplete-report.vcf.gz")
                                                    .toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.FAILED, stepContribution.getExitStatus());
        assertEquals(1, reportCheckTasklet.getUnmatchedVariantsInInputVcf());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInReportVcf());
    }

    @Test
    public void reportContainsAccessiontNotPresentInOriginalVcf() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.unexpected-report.vcf.gz")
                                                    .toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.FAILED, stepContribution.getExitStatus());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInInputVcf());
        assertEquals(1, reportCheckTasklet.getUnmatchedVariantsInReportVcf());
    }

    @Test
    public void originalVcfContainsNonVariants() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/genotyped.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/genotyped.report.vcf.gz")
                                                    .toURI();
        ReportCheckTasklet reportCheckTasklet = getGenotypedReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
        assertEquals(1, reportCheckTasklet.getSkippedVariantsInInputVcf());
        assertEquals(0, reportCheckTasklet.getSkippedVariantsInReportVcf());
    }

    private ReportCheckTasklet getGenotypedReportCheckTasklet(URI vcfUri, URI reportUri) throws IOException {
        File vcfFile = new File(vcfUri);
        VcfReader vcfReader = new VcfReader("fileId", "studyId", vcfFile);
        UnwindingItemStreamReader<Variant> unwindingVcfReader = new UnwindingItemStreamReader<>(vcfReader);

        File reportFile = new File(reportUri);
        VcfReader reportReader = new VcfReader(new AccessionedVcfLineMapper(), reportFile);
        UnwindingItemStreamReader<Variant> unwindingReportReader = new UnwindingItemStreamReader<>(reportReader);

        return new ReportCheckTasklet(unwindingVcfReader, unwindingReportReader, 1000, contigMapping);
    }

    @Test
    public void vcfsContainDuplicates() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class
                .getResource("/input-files/vcf/aggregated.with_duplicates.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class
                .getResource("/input-files/vcf/aggregated.with_duplicates.report.vcf.gz").toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        // TODO: when we do left alignment using the reference fasta, the next asserts should be:
        // step is properly completed, 2 duplicates in input, 0 duplicates in report
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
        assertEquals(1, reportCheckTasklet.getDuplicatedVariantsInInputVcf());
        assertEquals(0, reportCheckTasklet.getDuplicatedVariantsInReportVcf());
    }

    @Test
    public void reportContainingContextBaseVariantShouldMatch() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class
                .getResource("/input-files/vcf/aggregated.with_ambiguous.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class
                .getResource("/input-files/vcf/aggregated.with_ambiguous.report.vcf.gz").toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInInputVcf());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInReportVcf());
    }

    @Test
    public void smallBuffer_10() throws Exception {
        profileBuffering(10, 40, 8);
    }

    @Test
    public void smallBuffer_11() throws Exception {
        profileBuffering(11, 22, 40);
    }

    @Test
    public void smallBuffer_20() throws Exception {
        profileBuffering(20, 40, 7);
    }

    @Test
    public void smallBuffer_21() throws Exception {
        profileBuffering(21, 21, 70);
    }

    /**
     * The report has all the accessions, but unordered. The 1st to 10th variants in the original vcf appear after
     * the 100th variant in the report, and the 11th to 20th in the original vcf appear at the end of the report.
     * @param initialBufferSize configures initial size of the buffers. If will grow if needed.
     * @param expectedMaxBufferSize expected max size of the buffers.
     * @param expectedIterations expected number of times a chunk was read from the VCFs.
     */
    private void profileBuffering(int initialBufferSize, int expectedMaxBufferSize,
                                  int expectedIterations) throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.report.vcf.gz").toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri, initialBufferSize);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
        assertEquals(expectedMaxBufferSize, reportCheckTasklet.getMaxBufferSize());
        assertEquals(expectedIterations, reportCheckTasklet.getIterations());
    }

    @Test
    public void checkWithContigSynonyms() throws Exception {
        IteratorItemStreamReader<Variant> inputReader = new IteratorItemStreamReader<>(
                Arrays.asList(new Variant("chromosome1", 100, 100, "A", "T")));
        IteratorItemStreamReader<Variant> reportReader = new IteratorItemStreamReader<>(
                Arrays.asList(new Variant("contig1", 100, 100, "A", "T")));

        contigMapping = new ContigMapping(Collections.singletonList(
                new ContigSynonyms("chromosome1", "assembled-molecule", "1", "contig1", "refseq1", "ucsc1", true)));

        ReportCheckTasklet reportCheckTasklet = new ReportCheckTasklet(inputReader, reportReader, 100, contigMapping);
        StepContribution stepContribution = new StepContribution(
                new StepExecution(QC_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInInputVcf());
        assertEquals(0, reportCheckTasklet.getUnmatchedVariantsInReportVcf());
    }

    private class IteratorItemStreamReader<T> implements ItemStreamReader<T> {

        Iterator<? extends T> iterator;

        public IteratorItemStreamReader(Iterable<? extends T> elements) {
            this.iterator = elements.iterator();
        }

        @Override
        public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
            if (iterator.hasNext()) {
                return iterator.next();
            } else {
                return null;
            }
        }

        @Override
        public void open(ExecutionContext executionContext) throws ItemStreamException {
        }

        @Override
        public void update(ExecutionContext executionContext) throws ItemStreamException {
        }

        @Override
        public void close() throws ItemStreamException {
        }
    }
}
