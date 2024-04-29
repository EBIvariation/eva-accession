package uk.ac.ebi.eva.accession.clustering.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.RSSplitWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.SSSplitWriter;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.JOB_EXECUTION_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.SS_SPLIT_WRITER;

@Configuration
public class JobProgressListener {
    @Bean(JOB_EXECUTION_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(NON_CLUSTERED_CLUSTERING_WRITER) ClusteringWriter nonClusteredClusteringWriter,
                                                     @Qualifier(CLUSTERED_CLUSTERING_WRITER) ClusteringWriter clusteredClusteringWriter,
                                                     @Qualifier(RS_SPLIT_WRITER) RSSplitWriter rsSplitWriter,
                                                     @Qualifier(SS_SPLIT_WRITER) SSSplitWriter ssSplitWriter) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                nonClusteredClusteringWriter.setJobExecution(jobExecution);
                clusteredClusteringWriter.setJobExecution(jobExecution);
                rsSplitWriter.setJobExecution(jobExecution);
                ssSplitWriter.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                // Do nothing
            }
        };
    }
}
