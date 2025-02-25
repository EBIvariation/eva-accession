package uk.ac.ebi.eva.accession.release.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.release.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.ActiveAccessionsVariantReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.DeprecatedAccessionsVariantReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.DumpRSAccessionsInFileConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.MergedAccessionsVariantReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.AccessionReleaseJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.ActiveAccessionReleaseFromDBJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.DeprecatedAccessionReleaseFromDBJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.DumpRSAccessionsJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.jobs.MergedAccessionReleaseFromDBJobConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.policies.IllegalStartSkipPolicyConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.ActiveAccessionReleaseFromDBStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.DeprecatedAccessionReleaseFromDBStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.DumpRSAccessionsStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.MergedAccessionReleaseFromDBStepConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_ACCESSIONS_RELEASE_FROM_DB_JOB;


@EnableAutoConfiguration
@Import({AccessionReleaseJobConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        IllegalStartSkipPolicyConfiguration.class,
        InputParametersConfiguration.class,
        DumpRSAccessionsJobConfiguration.class,
        DumpRSAccessionsStepConfiguration.class,
        DumpRSAccessionsInFileConfiguration.class,
        ActiveAccessionReleaseFromDBJobConfiguration.class,
        ActiveAccessionReleaseFromDBStepConfiguration.class,
        ActiveAccessionsVariantReaderConfiguration.class,
        DeprecatedAccessionReleaseFromDBJobConfiguration.class,
        DeprecatedAccessionReleaseFromDBStepConfiguration.class,
        DeprecatedAccessionsVariantReaderConfiguration.class,
        MergedAccessionReleaseFromDBJobConfiguration.class,
        MergedAccessionReleaseFromDBStepConfiguration.class,
        MergedAccessionsVariantReaderConfiguration.class
})
public class ReleaseFromDBTestConfiguration {

    public static final String TEST_DUMP_ACTIVE_ACCESSIONS_JOB = "TEST_DUMP_ACTIVE_ACCESSIONS_JOB";
    public static final String TEST_DUMP_DEPRECATED_ACCESSIONS_JOB = "TEST_DUMP_DEPRECATED_ACCESSIONS_JOB";
    public static final String TEST_DUMP_MERGED_ACCESSIONS_JOB = "TEST_DUMP_MERGED_ACCESSIONS_JOB";
    public static final String TEST_RELEASE_ACTIVE_ACCESSIONS_JOB = "TEST_RELEASE_ACTIVE_ACCESSIONS_JOB";
    public static final String TEST_RELEASE_DEPRECATED_ACCESSIONS_JOB = "TEST_RELEASE_DEPRECATED_ACCESSIONS_JOB";
    public static final String TEST_RELEASE_MERGED_ACCESSIONS_JOB = "TEST_RELEASE_MERGED_ACCESSIONS_JOB";

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }

    @Bean(TEST_DUMP_ACTIVE_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsActiveAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DUMP_ACTIVE_ACCESSIONS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(TEST_DUMP_DEPRECATED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsDeprecatedAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DUMP_DEPRECATED_ACCESSIONS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(TEST_DUMP_MERGED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsMergedAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DUMP_MERGED_ACCESSIONS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(TEST_RELEASE_ACTIVE_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsReleaseActiveAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(TEST_RELEASE_DEPRECATED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsReleaseDeprecatedAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(TEST_RELEASE_MERGED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsReleaseMergedAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(MERGED_ACCESSIONS_RELEASE_FROM_DB_JOB) Job job) {
                super.setJob(job);
            }
        };
    }
}
