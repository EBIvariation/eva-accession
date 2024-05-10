package uk.ac.ebi.eva.accession.clustering.configuration.batch.recovery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionRecoveryAgent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.MonotonicAccessionRecoveryAgentCategoryRSService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningDatabaseService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_SERVICE;

@Configuration
public class MonotonicAccessionRecoveryAgentCategoryRSServiceConfiguration {
    @Autowired
    private ContiguousIdBlockService blockService;
    @Autowired
    private ClusteredVariantAccessioningDatabaseService clusteredVariantAccessioningDatabaseService;

    @Value("${recovery.cutoff.days}")
    private long recoveryCutOffDays;

    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_SERVICE)
    public MonotonicAccessionRecoveryAgentCategoryRSService getMonotonicAccessionRecoveryAgentCategoryRSService() {
        return new MonotonicAccessionRecoveryAgentCategoryRSService(getMonotonicAccessionRecoveryAgent(), recoveryCutOffDays);
    }

    private MonotonicAccessionRecoveryAgent getMonotonicAccessionRecoveryAgent() {
        return new MonotonicAccessionRecoveryAgent(blockService, clusteredVariantAccessioningDatabaseService);
    }
}
