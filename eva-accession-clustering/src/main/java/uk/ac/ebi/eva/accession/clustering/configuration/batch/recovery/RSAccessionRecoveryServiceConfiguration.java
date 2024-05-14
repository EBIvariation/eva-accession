package uk.ac.ebi.eva.accession.clustering.configuration.batch.recovery;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionRecoveryAgent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.RSAccessionRecoveryService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningDatabaseService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_SERVICE;

@Configuration
public class RSAccessionRecoveryServiceConfiguration {
    @Autowired
    private ContiguousIdBlockService blockService;
    @Autowired
    private ClusteredVariantAccessioningDatabaseService clusteredVariantAccessioningDatabaseService;

    @Value("${recovery.cutoff.days}")
    private long recoveryCutOffDays;

    @Bean(RS_ACCESSION_RECOVERY_SERVICE)
    public RSAccessionRecoveryService getMonotonicAccessionRecoveryAgentCategoryRSService() {
        return new RSAccessionRecoveryService(getMonotonicAccessionRecoveryAgent(), recoveryCutOffDays);
    }

    private MonotonicAccessionRecoveryAgent getMonotonicAccessionRecoveryAgent() {
        return new MonotonicAccessionRecoveryAgent(blockService, clusteredVariantAccessioningDatabaseService);
    }
}
