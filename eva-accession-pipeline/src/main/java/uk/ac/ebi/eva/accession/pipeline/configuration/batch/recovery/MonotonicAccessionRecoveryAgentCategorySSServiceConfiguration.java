package uk.ac.ebi.eva.accession.pipeline.configuration.batch.recovery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionRecoveryAgent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.pipeline.batch.recovery.MonotonicAccessionRecoveryAgentCategorySSService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_SERVICE;

@Configuration
public class MonotonicAccessionRecoveryAgentCategorySSServiceConfiguration {
    @Autowired
    private ContiguousIdBlockService blockService;
    @Autowired
    private SubmittedVariantAccessioningDatabaseService submittedVariantAccessioningDatabaseService;

    @Value("${recovery.cutoff.days}")
    private long recoveryCutOffDays;

    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_SERVICE)
    public MonotonicAccessionRecoveryAgentCategorySSService getMonotonicAccessionRecoveryAgentCategorySSService() {
        return new MonotonicAccessionRecoveryAgentCategorySSService(getMonotonicAccessionRecoveryAgent(), recoveryCutOffDays);
    }

    private MonotonicAccessionRecoveryAgent getMonotonicAccessionRecoveryAgent() {
        return new MonotonicAccessionRecoveryAgent(blockService, submittedVariantAccessioningDatabaseService);
    }
}
