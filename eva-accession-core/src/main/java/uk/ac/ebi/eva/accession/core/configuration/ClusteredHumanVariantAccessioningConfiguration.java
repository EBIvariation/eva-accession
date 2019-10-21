package uk.ac.ebi.eva.accession.core.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.repositoryHuman.DbsnpClusteredHumanVariantAccessioningDatabaseService;
import uk.ac.ebi.eva.accession.core.repositoryHuman.DbsnpHumanClusteredVariantAccessionRepository;
import uk.ac.ebi.eva.accession.core.service.DbsnpClusteredVariantInactiveService;

@Configuration
@Import({MongoHumanConfiguration.class})
public class ClusteredHumanVariantAccessioningConfiguration {

    @Autowired
    private DbsnpHumanClusteredVariantAccessionRepository dbsnpHumanRepository;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Autowired
    private ContiguousIdBlockService service;

    @Autowired
    private DbsnpClusteredVariantInactiveService dbsnpInactiveService;

    @Bean("humanService1")
    public ClusteredVariantAccessioningService clusteredHumanVariantAccessioningService() {
        return new ClusteredVariantAccessioningService(dbsnpClusteredVariantAccessionGenerator(),
                                                       dbsnpClusteredVariantAccessioningDatabaseService());
    }

    private DbsnpMonotonicAccessionGenerator<IClusteredVariant> dbsnpClusteredVariantAccessionGenerator() {
        ApplicationProperties properties = applicationProperties;
        return new DbsnpMonotonicAccessionGenerator<>(properties.getClustered().getCategoryId(),
                                                      properties.getInstanceId(), service);
    }

    private DbsnpClusteredHumanVariantAccessioningDatabaseService dbsnpClusteredVariantAccessioningDatabaseService() {
        return new DbsnpClusteredHumanVariantAccessioningDatabaseService(dbsnpHumanRepository, dbsnpInactiveService);
    }

}
