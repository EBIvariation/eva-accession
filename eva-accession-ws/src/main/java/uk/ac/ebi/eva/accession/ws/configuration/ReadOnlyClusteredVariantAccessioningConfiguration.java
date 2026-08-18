package uk.ac.ebi.eva.accession.ws.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationPropertiesConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.ContigAliasConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantOperationService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantInactiveService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.service.dbsnp.DbsnpReadOnlyClusteredVariantService;
import uk.ac.ebi.eva.accession.ws.service.eva.EvaReadOnlyClusteredVariantService;
import uk.ac.ebi.eva.accession.ws.service.ReadOnlyClusteredVariantService;

/**
 * Equivalent to {@link uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration}
 * with accession generators removed and accessioning services replaced by read-only equivalents.
 */
@Configuration
@Import({MongoConfiguration.class, ContigAliasConfiguration.class})
public class ReadOnlyClusteredVariantAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ReadOnlyClusteredVariantAccessioningConfiguration.class);

    @Autowired
    private ClusteredVariantAccessioningRepository repository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private ClusteredVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository dbsnpOperationRepository;

    @Autowired
    private ContigAliasService contigAliasService;

    @Value("${accessioning.monotonic.rs.blockStartValue}")
    private Long accessioningMonotonicInitRs;

    @Bean("nonhumanReadOnlyActiveService")
    public ReadOnlyClusteredVariantService readOnlyClusteredVariantService() {
        return new ReadOnlyClusteredVariantService(evaReadOnlyClusteredVariantService(),
                dbsnpReadOnlyClusteredVariantService(),
                accessioningMonotonicInitRs, contigAliasService);
    }

    private EvaReadOnlyClusteredVariantService evaReadOnlyClusteredVariantService() {
        return new EvaReadOnlyClusteredVariantService(
                clusteredVariantAccessioningDatabaseService(),
                new ClusteredVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    private DbsnpReadOnlyClusteredVariantService dbsnpReadOnlyClusteredVariantService() {
        return new DbsnpReadOnlyClusteredVariantService(
                dbsnpClusteredVariantAccessioningDatabaseService(),
                new ClusteredVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    @Bean
    public ClusteredVariantAccessioningDatabaseService clusteredVariantAccessioningDatabaseService() {
        return new ClusteredVariantAccessioningDatabaseService(repository, clusteredVariantInactiveService());
    }

    @Bean
    public DbsnpClusteredVariantAccessioningDatabaseService dbsnpClusteredVariantAccessioningDatabaseService() {
        return new DbsnpClusteredVariantAccessioningDatabaseService(dbsnpRepository,
                dbsnpClusteredVariantInactiveService());
    }

    @Bean
    public ClusteredVariantOperationService clusteredVariantHistoryService() {
        return new ClusteredVariantOperationService(dbsnpClusteredVariantInactiveService(),
                clusteredVariantInactiveService(),
                contigAliasService);
    }

    @Bean
    public ClusteredVariantOperationRepository clusteredVariantOperationRepository() {
        return operationRepository;
    }

    @Bean
    public DbsnpClusteredVariantOperationRepository dbsnpClusteredVariantOperationRepository() {
        return dbsnpOperationRepository;
    }

    @Bean
    public ClusteredVariantInactiveService clusteredVariantInactiveService() {
        return new ClusteredVariantInactiveService(operationRepository, ClusteredVariantInactiveEntity::new,
                ClusteredVariantOperationEntity::new);
    }

    @Bean("nonhumanInactiveService")
    public DbsnpClusteredVariantInactiveService dbsnpClusteredVariantInactiveService() {
        return new DbsnpClusteredVariantInactiveService(dbsnpOperationRepository,
                DbsnpClusteredVariantInactiveEntity::new,
                DbsnpClusteredVariantOperationEntity::new);
    }
}
