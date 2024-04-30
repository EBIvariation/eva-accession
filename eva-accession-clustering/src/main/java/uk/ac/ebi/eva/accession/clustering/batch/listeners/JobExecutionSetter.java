package uk.ac.ebi.eva.accession.clustering.batch.listeners;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.RSSplitWriter;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER_JOB_EXECUTION_SETTER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_CLUSTERING_WRITER_JOB_EXECUTION_SETTER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER_JOB_EXECUTION_SETTER;

@Configuration
public class JobExecutionSetter {
    @Bean(NON_CLUSTERED_CLUSTERING_WRITER_JOB_EXECUTION_SETTER)
    public StepExecutionListener getNonClusteredClusteringWriterJobExecutionSetter(
            @Qualifier(NON_CLUSTERED_CLUSTERING_WRITER) ClusteringWriter nonClusteredClusteringWriter) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                nonClusteredClusteringWriter.setJobExecution(stepExecution.getJobExecution());
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                return stepExecution.getExitStatus();
            }
        };
    }

    @Bean(CLUSTERED_CLUSTERING_WRITER_JOB_EXECUTION_SETTER)
    public StepExecutionListener getClusteredClusteringWriterJobExecutionSetter(
            @Qualifier(CLUSTERED_CLUSTERING_WRITER) ClusteringWriter clusteredClusteringWriter) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                clusteredClusteringWriter.setJobExecution(stepExecution.getJobExecution());
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                return stepExecution.getExitStatus();
            }
        };
    }


    @Bean(RS_SPLIT_WRITER_JOB_EXECUTION_SETTER)
    public StepExecutionListener getRSSplitWriterJobExecutionSetter(
            @Qualifier(RS_SPLIT_WRITER) RSSplitWriter rsSplitWriter) {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                rsSplitWriter.setJobExecution(stepExecution.getJobExecution());
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                return stepExecution.getExitStatus();
            }
        };
    }
}
