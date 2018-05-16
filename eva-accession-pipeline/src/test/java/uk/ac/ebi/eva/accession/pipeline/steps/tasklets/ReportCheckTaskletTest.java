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

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;

import uk.ac.ebi.eva.commons.batch.io.AggregatedVcfReader;
import uk.ac.ebi.eva.commons.core.models.Aggregation;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;

public class ReportCheckTaskletTest {

    private static final long JOB_ID = 0L;

    @Test
    public void correctReport() throws Exception {
        // given
        File vcfFile = new File(ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI());
        File reportFile = new File(ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.report" +
                                                                                       ".vcf.gz").toURI());
        AggregatedVcfReader vcfReader = new AggregatedVcfReader("fileId", "studyId", Aggregation.BASIC, null, vcfFile);
        AggregatedVcfReader reportReader = new AggregatedVcfReader("reportFile", "studyId", Aggregation.BASIC, null,
                                                                   reportFile);

        ReportCheckTasklet reportCheckTasklet = new ReportCheckTasklet(vcfReader, reportReader);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.COMPLETED, stepContribution.getExitStatus());
    }

    @Test
    public void variantMissingInReport() throws Exception {
        // given
        File vcfFile = new File(ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.vcf.gz").toURI());
        File reportFile = new File(
                ReportCheckTaskletTest.class.getResource("/input-files/vcf/aggregated.wrong-report.vcf.gz").toURI());
        AggregatedVcfReader vcfReader = new AggregatedVcfReader("fileId", "studyId", Aggregation.BASIC, null, vcfFile);
        AggregatedVcfReader reportReader = new AggregatedVcfReader("reportFile", "studyId", Aggregation.BASIC, null,
                                                                   reportFile);

        ReportCheckTasklet reportCheckTasklet = new ReportCheckTasklet(vcfReader, reportReader);

        // when
        StepContribution stepContribution = new StepContribution(
                new StepExecution(CHECK_SUBSNP_ACCESSION_STEP, new JobExecution(JOB_ID)));
        reportCheckTasklet.execute(stepContribution, null);

        // then
        assertEquals(ExitStatus.FAILED, stepContribution.getExitStatus());
    }
}
