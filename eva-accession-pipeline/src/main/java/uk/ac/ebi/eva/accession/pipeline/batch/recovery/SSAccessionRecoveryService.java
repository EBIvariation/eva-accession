package uk.ac.ebi.eva.accession.pipeline.batch.recovery;

import org.springframework.batch.core.JobExecution;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionRecoveryAgent;

import java.time.LocalDateTime;

public class SSAccessionRecoveryService {
    private final static String CATEGORY_ID = "ss";
    private MonotonicAccessionRecoveryAgent monotonicAccessionRecoveryAgent;
    private JobExecution jobExecution;
    private long recoveryCutOffDays;

    public SSAccessionRecoveryService(MonotonicAccessionRecoveryAgent monotonicAccessionRecoveryAgent,
                                      long recoveryCutOffDays) {
        this.monotonicAccessionRecoveryAgent = monotonicAccessionRecoveryAgent;
        this.recoveryCutOffDays = recoveryCutOffDays;
    }

    public void runRecoveryForCategorySS() {
        LocalDateTime recoveryCutOffTime = LocalDateTime.now().minusDays(recoveryCutOffDays);
        monotonicAccessionRecoveryAgent.runRecovery(CATEGORY_ID, jobExecution.getJobId().toString(), recoveryCutOffTime);
    }

    public void setJobExecution(JobExecution jobExecution) {
        this.jobExecution = jobExecution;
    }
}
