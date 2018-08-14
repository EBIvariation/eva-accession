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
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.JobLauncherCommandLineRunner;
import org.springframework.stereotype.Component;

import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_JOB;

@Component
public class DbsnpImportVariantsJobLauncherCommandLineRunner extends JobLauncherCommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpImportVariantsJobLauncherCommandLineRunner.class);

    @Autowired
    @Qualifier(IMPORT_DBSNP_VARIANTS_JOB)
    private Job importDbsnpVariantsJob;

    @Autowired
    private InputParameters inputParameters;

    public DbsnpImportVariantsJobLauncherCommandLineRunner(JobLauncher jobLauncher,
                                                           JobExplorer jobExplorer) {
        super(jobLauncher, jobExplorer);
    }

    @Override
    public void run(String... args) throws JobExecutionException {
        JobParameters jobParameters = inputParameters.toJobParameters();
        this.execute(importDbsnpVariantsJob, jobParameters);
    }

    @Override
    public void execute(Job job,
                        JobParameters jobParameters) throws JobParametersNotFoundException,
            JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException,
            JobInstanceAlreadyCompleteException {
        logger.info("Running job '" + job.getName() + "' with parameters: " + jobParameters);
        super.execute(job, jobParameters);
    }
}
