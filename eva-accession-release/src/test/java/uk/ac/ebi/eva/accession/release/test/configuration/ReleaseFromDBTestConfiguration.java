package uk.ac.ebi.eva.accession.release.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.release.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.ActiveAccessionsVariantReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.DumpRSAccessionsInFileConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.MergedAndDeprecatedAccessionsVariantReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.VariantContextWriterConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.ActiveAccessionReleaseFromDBJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.DumpRSAccessionsJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.MergedAndDeprecatedAccessionReleaseFromDBJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.policies.IllegalStartSkipPolicyConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.processors.ReleaseProcessorConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.ActiveAccessionReleaseFromDBStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.DumpRSAccessionsStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.MergedAndDeprecatedAccessionReleaseFromDBStepConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_AND_DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB;


@EnableAutoConfiguration
@Import({ChunkSizeCompletionPolicyConfiguration.class,
        IllegalStartSkipPolicyConfiguration.class,
        InputParametersConfiguration.class,
        ReleaseProcessorConfiguration.class,
        VariantContextWriterConfiguration.class,
        DumpRSAccessionsJobConfiguration.class,
        DumpRSAccessionsStepConfiguration.class,
        DumpRSAccessionsInFileConfiguration.class,
        ActiveAccessionReleaseFromDBJobConfiguration.class,
        ActiveAccessionReleaseFromDBStepConfiguration.class,
        ActiveAccessionsVariantReaderConfiguration.class,
        MergedAndDeprecatedAccessionReleaseFromDBJobConfiguration.class,
        MergedAndDeprecatedAccessionReleaseFromDBStepConfiguration.class,
        MergedAndDeprecatedAccessionsVariantReaderConfiguration.class
})
public class ReleaseFromDBTestConfiguration {
    public static final String TEST_DUMP_ACTIVE_ACCESSIONS_JOB = "TEST_DUMP_ACTIVE_ACCESSIONS_JOB";
    public static final String TEST_DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB = "TEST_DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB";
    public static final String TEST_RELEASE_ACTIVE_ACCESSIONS_JOB = "TEST_RELEASE_ACTIVE_ACCESSIONS_JOB";
    public static final String TEST_RELEASE_MERGED_AND_DEPRECATED_ACCESSIONS_JOB = "TEST_RELEASE_MERGED_AND_DEPRECATED_ACCESSIONS_JOB";

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }

    @Bean(TEST_DUMP_ACTIVE_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsActiveAccessions(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                     @Qualifier(DUMP_ACTIVE_ACCESSIONS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(TEST_DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsMergedAccessions(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                     @Qualifier(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;

    }

    @Bean(TEST_RELEASE_ACTIVE_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsReleaseActiveAccessions(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                            @Qualifier(ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;

    }

    @Bean(TEST_RELEASE_MERGED_AND_DEPRECATED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsReleaseMergedAccessions(JobLauncher jobLauncher, JobRepository jobRepository,
                                                                            @Qualifier(MERGED_AND_DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;

    }
}
