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
package uk.ac.ebi.eva.accession.dbsnp.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Entity;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;

import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.exception.NoJobToExecuteException;
import uk.ac.ebi.eva.commons.batch.exception.NoParametersHaveBeenPassedException;
import uk.ac.ebi.eva.commons.batch.exception.NoPreviousJobExecutionException;
import uk.ac.ebi.eva.commons.batch.exception.UnknownJobException;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;
import uk.ac.ebi.eva.commons.batch.job.JobStatusManager;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

@Component
public class DbsnpImportVariantsJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner implements
        ApplicationEventPublisherAware, ExitCodeGenerator {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpImportVariantsJobLauncherCommandLineRunner.class);

    public static final String SPRING_BATCH_JOB_NAME_PROPERTY = "spring.batch.job.names";

    public static final int EXIT_WITHOUT_ERRORS = 0;

    public static final int EXIT_WITH_ERRORS = 1;

    private final JobExplorer jobExplorer;

    @Value("${" + SPRING_BATCH_JOB_NAME_PROPERTY + ":#{null}}")
    private String jobName;

    private Collection<Job> jobs;

    private JobRepository jobRepository;

    private String RUN_ID_PARAMETER_NAME = "run.id";

    @Autowired
    private JobExecutionApplicationListener jobExecutionApplicationListener;

    @Autowired
    private InputParameters inputParameters;

    private boolean abnormalExit;

    public DbsnpImportVariantsJobLauncherCommandLineRunner(JobLauncher jobLauncher, JobExplorer jobExplorer,
                                                           JobRepository jobRepository) {
        super(jobLauncher, jobExplorer, jobRepository);
        this.jobExplorer = jobExplorer;
        this.jobRepository = jobRepository;
    }


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

            // TODO: different jobs can have different parameters
            JobParameters jobParameters = inputParameters.toJobParameters();
            JobStatusManager.checkIfJobNameHasBeenDefined(jobName);
            JobStatusManager.checkIfPropertiesHaveBeenProvided(jobParameters);
            if (inputParameters.isForceRestart()) {
                markPreviousJobAsFailed(jobParameters);
            }
            else {
                jobParameters = addRunIDToJobParameters(jobParameters);
            }
            launchJob(jobParameters);
        } catch (NoJobToExecuteException | NoParametersHaveBeenPassedException | NoPreviousJobExecutionException
                | UnknownJobException | JobParametersInvalidException | JobExecutionAlreadyRunningException e) {
            logger.error(e.getMessage());
            logger.debug("Error trace", e);
            abnormalExit = true;
        }

    }

    private JobParameters addRunIDToJobParameters(JobParameters jobParameters) {
        JobExecution lastJobExecution = getLastJobExecution();
        if (lastJobExecution != null) {
            Long runIdParameterFromLastExecution = lastJobExecution.getJobParameters()
                                                                           .getLong(RUN_ID_PARAMETER_NAME);
            if (runIdParameterFromLastExecution != 0 && lastJobExecution.getStatus() == BatchStatus.FAILED) {
                // Spring Batch 4 uses all job parameters (including run.id) to detect previous instances of a job - see https://github.com/spring-projects/spring-boot/blob/86fb39d5c5f474fe3544159270d4c4e2d01d43ef/spring-boot-project/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/batch/JobLauncherCommandLineRunner.java#L222
                // as opposed to Spring 3 which uses only job name - see https://github.com/spring-projects/spring-boot/blob/541890f0e003a6e346f2234102c97105ab1292ee/spring-boot-autoconfigure/src/main/java/org/springframework/boot/autoconfigure/batch/JobLauncherCommandLineRunner.java#L131
                // Therefore, run.id needs to be supplied for failed jobs in order for the job to be detected and resumed - see https://stackoverflow.com/a/59198742/2601814
                return new JobParametersBuilder(jobParameters)
                        .addLong(RUN_ID_PARAMETER_NAME, runIdParameterFromLastExecution).toJobParameters();
            }
        }

        return jobParameters;
    }

    private JobExecution getLastJobExecution () {
        int previousJobInstanceCount = getPreviousJobInstanceCount();
        List<JobInstance> jobInstanceList = jobExplorer.getJobInstances(jobName, 0, previousJobInstanceCount);
        if (!jobInstanceList.isEmpty()) {
            return jobExplorer.getJobExecutions(jobInstanceList.get(0)).stream()
                              .max(Comparator.comparingLong(Entity::getId)).get();
        }
        return null;
    }
    
    private int getPreviousJobInstanceCount() {
        try {
            return jobExplorer.getJobInstanceCount(jobName);
        }
        catch (NoSuchJobException ex) {
            return 0;
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

        throw new UnknownJobException(jobName);
    }

    @Override
    protected void execute(Job job, JobParameters jobParameters) throws JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException,
            JobParametersNotFoundException {
        logger.info("Running job '" + jobName + "' with parameters: " + jobParameters);
        super.execute(job, jobParameters);
    }

    private void markPreviousJobAsFailed(JobParameters jobParameters) throws
            NoPreviousJobExecutionException {
        logger.info("Force restartPreviousExecution of job '" + jobName + "' with parameters: " + jobParameters);
        JobStatusManager.markLastJobAsFailed(jobRepository, jobName, jobParameters);
    }
}
