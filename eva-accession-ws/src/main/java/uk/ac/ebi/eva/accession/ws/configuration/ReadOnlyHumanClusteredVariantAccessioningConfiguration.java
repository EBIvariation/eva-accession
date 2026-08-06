package uk.ac.ebi.eva.accession.ws.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.configuration.ContigAliasConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.human.HumanMongoConfiguration;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.HumanDbsnpClusteredVariantAccessionRepository;
import uk.ac.ebi.eva.accession.core.repository.human.dbsnp.HumanDbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantOperationAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.service.DbsnpReadOnlyHumanClusteredVariantService;
import uk.ac.ebi.eva.accession.ws.service.ReadOnlyHumanClusteredVariantService;

@Import({HumanMongoConfiguration.class, ContigAliasConfiguration.class})
public class ReadOnlyHumanClusteredVariantAccessioningConfiguration {

    @Autowired
    private HumanDbsnpClusteredVariantAccessionRepository humanDbsnpRepository;

    @Autowired
    private HumanDbsnpClusteredVariantOperationRepository humanDbsnpClusteredVariantOperationRepository;

    @Autowired
    private ContigAliasService contigAliasService;

    @Bean("humanActiveService")
    public DbsnpReadOnlyHumanClusteredVariantService humanDbsnpClusteredActiveVariantAccessioningService() {
        return new DbsnpReadOnlyHumanClusteredVariantService(
                humanDbsnpClusteredVariantAccessioningDatabaseService(),
                new ClusteredVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    private HumanDbsnpClusteredVariantAccessioningDatabaseService humanDbsnpClusteredVariantAccessioningDatabaseService() {
        return new HumanDbsnpClusteredVariantAccessioningDatabaseService(humanDbsnpRepository,
                humanDbsnpClusteredVariantInactiveService());
    }

    @Bean("humanOperationsService")
    public HumanDbsnpClusteredVariantOperationAccessioningService humanDbsnpClusteredVariantOperationAccessioningService() {
        return new HumanDbsnpClusteredVariantOperationAccessioningService(humanDbsnpClusteredVariantOperationRepository);
    }

    @Bean("humanReadOnlyService")
    public ReadOnlyHumanClusteredVariantService humanDbsnpClusteredVariantAccessioningService() {
        return new ReadOnlyHumanClusteredVariantService(humanDbsnpClusteredActiveVariantAccessioningService(),
                humanDbsnpClusteredVariantOperationAccessioningService(),
                contigAliasService);
    }

    @Bean("humanInactiveService")
    public DbsnpClusteredVariantInactiveService humanDbsnpClusteredVariantInactiveService() {
        return new DbsnpClusteredVariantInactiveService(humanDbsnpClusteredVariantOperationRepository,
                DbsnpClusteredVariantInactiveEntity::new,
                DbsnpClusteredVariantOperationEntity::new);
    }
}
