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

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;

import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfReader;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;

public class ReportCheckTaskletTest {

    private static final long JOB_ID = 0L;

    @Test
    public void correctReport() throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.report.vcf.gz").toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
    }

    private ReportCheckTasklet getReportCheckTasklet(URI vcfUri, URI reportUri) throws IOException {
        File vcfFile = new File(vcfUri);
        AggregatedVcfReader vcfReader = new AggregatedVcfReader("fileId", "studyId", Aggregation.BASIC, null, vcfFile);
        UnwindingItemStreamReader<Variant> unwindingVcfReader = new UnwindingItemStreamReader<>(vcfReader);

        File reportFile = new File(reportUri);
        VcfReader reportReader = new VcfReader(new CoordinatesVcfLineMapper(), reportFile);
        UnwindingItemStreamReader<Variant> unwindingReportReader = new UnwindingItemStreamReader<>(reportReader);

        return new ReportCheckTasklet(unwindingVcfReader, unwindingReportReader);
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
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.FAILED, stepContribution.getExitStatus());
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
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.FAILED, stepContribution.getExitStatus());
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
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
    }

    private ReportCheckTasklet getGenotypedReportCheckTasklet(URI vcfUri, URI reportUri) throws IOException {
        File vcfFile = new File(vcfUri);
        VcfReader vcfReader = new VcfReader("fileId", "studyId", vcfFile);
        UnwindingItemStreamReader<Variant> unwindingVcfReader = new UnwindingItemStreamReader<>(vcfReader);

        File reportFile = new File(reportUri);
        VcfReader reportReader = new VcfReader(new CoordinatesVcfLineMapper(), reportFile);
        UnwindingItemStreamReader<Variant> unwindingReportReader = new UnwindingItemStreamReader<>(reportReader);

        return new ReportCheckTasklet(unwindingVcfReader, unwindingReportReader);
    }

    @Test
    public void smallBuffer_10() throws Exception {
        profileBuffering(10, 136, 137);
    }

    @Test
    public void smallBuffer_11() throws Exception {
        profileBuffering(11, 81, 137);
    }

    @Test
    public void smallBuffer_20() throws Exception {
        profileBuffering(20, 81, 95);
    }

    @Test
    public void smallBuffer_21() throws Exception {
        profileBuffering(21, 1, 86);
    }

    /**
     * The report has all the accessions, but unordered. The 1st to 10th variants in the original vcf appear after
     * the 100th variant in the report, and the 11th to 20th in the original vcf appear at the end of the report.
     * @param maxVariantBufferSize configures max size of the variantBuffer, which is filled from the original VCF.
     * @param expectedMaxUnmatchedAccessionsBufferSize expected max size of the set of accessions
     * @param expectedIterations expected number of times a chunk was read from the original VCF
     */
    private void profileBuffering(int maxVariantBufferSize, int expectedMaxUnmatchedAccessionsBufferSize,
                                  int expectedIterations) throws Exception {
        // given
        URI vcfUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI();
        URI reportUri = ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.report.vcf.gz").toURI();
        ReportCheckTasklet reportCheckTasklet = getReportCheckTasklet(vcfUri, reportUri);
        reportCheckTasklet.setMaxVariantBufferSize(maxVariantBufferSize);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
        assertEquals(expectedMaxUnmatchedAccessionsBufferSize, reportCheckTasklet.getMaxAccessionBufferSize());
        assertEquals(expectedIterations, reportCheckTasklet.getIterations());
    }
}
