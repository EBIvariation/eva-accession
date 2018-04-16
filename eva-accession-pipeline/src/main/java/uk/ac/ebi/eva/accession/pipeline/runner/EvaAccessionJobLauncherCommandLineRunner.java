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
package uk.ac.ebi.eva.accession.pipeline.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;

import uk.ac.ebi.eva.accession.pipeline.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.exception.NoJobToExecuteException;
import uk.ac.ebi.eva.commons.batch.exception.NoParametersHaveBeenPassedException;
import uk.ac.ebi.eva.commons.batch.exception.NoPreviousJobExecutionException;
import uk.ac.ebi.eva.commons.batch.exception.UnknownJobException;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;
import uk.ac.ebi.eva.commons.batch.job.ManageJobsUtils;

import java.util.Collection;

/**
 * This class is a modified version of the default JobLauncherCommandLineRunner.
 * Its main differences are:
 * -If no job is specified then the execution stops.
 * // TODO: remove the next two lines
// * -Job parameters can be passed from command line as normal parameters.
// * -Job parameters can be passed from a properties file by the user.
 * -The user can restart a job that has been run previously marking the previous execution as failed.
 */
@Component
public class EvaAccessionJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner implements
        ApplicationEventPublisherAware, ExitCodeGenerator {

    private static final Logger logger = LoggerFactory.getLogger(EvaAccessionJobLauncherCommandLineRunner.class);

    // TODO: move those constants to variation-commons-batch?
    public static final String SPRING_BATCH_JOB_NAME_PROPERTY = "spring.batch.job.names";

    public static final String RESTART_PROPERTY = "force.restart";

    public static final int EXIT_WITHOUT_ERRORS = 0;

    public static final int EXIT_WITH_ERRORS = 1;

    @Value("${" + SPRING_BATCH_JOB_NAME_PROPERTY + ":#{null}}")
    private String jobName;

    @Value("${" + RESTART_PROPERTY + ":false}")
    private boolean restartPreviousExecution;

    private Collection<Job> jobs;

    private JobRepository jobRepository;

//    private JobRegistry jobRegistry;

    @Autowired
    private JobExecutionApplicationListener jobExecutionApplicationListener;

    @Autowired
    private InputParameters inputParameters;

    private boolean abnormalExit;

    public EvaAccessionJobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer) {
//                                                    JobRepository jobRepository) {
        super(jobLauncher, jobExplorer);
//        jobs = Collections.emptySet();
        this.jobRepository = jobRepository;
//        converter = new DefaultJobParametersConverter();

    }

//    @Autowired(required = false)
//    public void setJobRegistry(JobRegistry jobRegistry) {
//        this.jobRegistry = jobRegistry;
//    }

    @Autowired(required = false)
    public void setJobs(Collection<Job> jobs) {
        this.jobs = jobs;
    }

    @Override
    public void setJobNames(String jobName) {
        this.jobName = jobName;
        super.setJobNames(jobName);
    }

    @Override
    public int getExitCode() {
        // TODO: do we really need to catch the exceptions, set abnormalExit and return 1? does not Spring batch do that?
        if (!abnormalExit && jobExecutionApplicationListener.isJobExecutionComplete()) {
            return EXIT_WITHOUT_ERRORS;
        } else {
            return EXIT_WITH_ERRORS;
        }
    }

    @Override
    public void run(String... args) throws JobExecutionException {
        try {
            abnormalExit = false;
            // TODO: exclude those parameters if they are in inputParameters class:
       // Filter all runner specific parameters
//        properties.remove(SPRING_BATCH_JOB_NAME_PROPERTY);
//        properties.remove(JobParametersNames.PROPERTY_FILE_PROPERTY);
//        properties.remove(JobParametersNames.RESTART_PROPERTY);
//
            JobParameters jobParameters = inputParameters.toJobParameters();

            ManageJobsUtils.checkIfJobNameHasBeenDefined(jobName);
            ManageJobsUtils.checkIfPropertiesHaveBeenProvided(jobParameters);
            if (restartPreviousExecution) {
                markPreviousJobAsFailed(jobParameters);
            }
            launchJob(jobParameters);
        } catch (NoJobToExecuteException | NoParametersHaveBeenPassedException | NoPreviousJobExecutionException
                | UnknownJobException | JobParametersInvalidException e) {
            logger.error(e.getMessage());
            logger.debug("Error trace", e);
            abnormalExit = true;
        }

    }

    private void launchJob(JobParameters jobParameters) throws JobExecutionException, UnknownJobException {
        for (Job job : this.jobs) {
            // TODO is PatternMatchUtils necessary because the jobs discovered by Spring include package name? if not, remove it
            if (PatternMatchUtils.simpleMatch(jobName, job.getName())) {
                execute(job, jobParameters);
                return;
            }
        }

        // TODO do we need to use the job registry?
//        if (this.jobRegistry != null) {
//            try {
//                execute(jobRegistry.getJob(jobName), jobParameters);
//            } catch (NoSuchJobException ex) {
//                logger.error("No job found in registry for job name: " + jobName);
//            }
//        }

        throw new UnknownJobException(jobName);
    }

    @Override
    protected void execute(Job job, JobParameters jobParameters) throws JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException,
            JobParametersNotFoundException {
        //TODO uncomment the log message
        logger.info("Running TEST job with parameters: " + jobParameters);
//        logger.info("Running job '" + jobName + "' with parameters: " + jobParameters);
        super.execute(job, jobParameters);
    }

    private void markPreviousJobAsFailed(JobParameters jobParameters) throws
            NoPreviousJobExecutionException {
        logger.info("Force restartPreviousExecution of job '" + jobName + "' with parameters: " + jobParameters);
        ManageJobsUtils.markLastJobAsFailed(jobRepository, jobName, jobParameters);
    }
}
