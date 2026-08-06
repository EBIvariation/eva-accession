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
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantInactiveService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.service.DbsnpReadOnlySubmittedVariantService;
import uk.ac.ebi.eva.accession.ws.service.EvaReadOnlySubmittedVariantService;
import uk.ac.ebi.eva.accession.ws.service.ReadOnlySubmittedVariantService;

@Configuration
@Import({ApplicationPropertiesConfiguration.class, MongoConfiguration.class, ContigAliasConfiguration.class})
public class ReadOnlySubmittedVariantAccessioningConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ReadOnlySubmittedVariantAccessioningConfiguration.class);

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository dbsnpRepository;

    @Autowired
    private SubmittedVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpSubmittedVariantOperationRepository dbsnpOperationRepository;

    @Autowired
    private ContigAliasService contigAliasService;

    @Value("${accessioning.monotonic.ss.blockStartValue}")
    private Long accessioningMonotonicInitSs;

    @Bean
    public ReadOnlySubmittedVariantService readOnlySubmittedVariantService() {
        return new ReadOnlySubmittedVariantService(evaReadOnlySubmittedVariantService(),
                dbsnpReadOnlySubmittedVariantService(),
                accessioningMonotonicInitSs, contigAliasService);
    }

    private EvaReadOnlySubmittedVariantService evaReadOnlySubmittedVariantService() {
        return new EvaReadOnlySubmittedVariantService(
                submittedVariantAccessioningDatabaseService(),
                new SubmittedVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    private DbsnpReadOnlySubmittedVariantService dbsnpReadOnlySubmittedVariantService() {
        return new DbsnpReadOnlySubmittedVariantService(
                dbsnpSubmittedVariantAccessioningDatabaseService(),
                new SubmittedVariantSummaryFunction(),
                new SHA1HashingFunction());
    }

    @Bean
    public SubmittedVariantAccessioningDatabaseService submittedVariantAccessioningDatabaseService() {
        return new SubmittedVariantAccessioningDatabaseService(repository, submittedVariantInactiveService());
    }

    @Bean
    public DbsnpSubmittedVariantAccessioningDatabaseService dbsnpSubmittedVariantAccessioningDatabaseService() {
        return new DbsnpSubmittedVariantAccessioningDatabaseService(dbsnpRepository, dbsnpSubmittedVariantInactiveService());
    }

    @Bean
    public SubmittedVariantOperationRepository submittedVariantOperationRepository() {
        return operationRepository;
    }

    @Bean
    public DbsnpSubmittedVariantOperationRepository dbsnpSubmittedVariantOperationRepository() {
        return dbsnpOperationRepository;
    }

    @Bean
    public SubmittedVariantInactiveService submittedVariantInactiveService() {
        return new SubmittedVariantInactiveService(operationRepository,
                SubmittedVariantInactiveEntity::new,
                SubmittedVariantOperationEntity::new);
    }

    @Bean
    public DbsnpSubmittedVariantInactiveService dbsnpSubmittedVariantInactiveService() {
        return new DbsnpSubmittedVariantInactiveService(dbsnpOperationRepository,
                DbsnpSubmittedVariantInactiveEntity::new,
                DbsnpSubmittedVariantOperationEntity::new);
    }
}
