package uk.ac.ebi.eva.accession.core.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.repositoryHuman.DbsnpHumanClusteredVariantAccessionRepository;

@Configuration
@Import({MongoHumanConfiguration.class})
public class ClusteredHumanVariantAccessioningConfiguration {

    @Autowired
    private DbsnpHumanClusteredVariantAccessionRepository dbsnpHumanRepository;

}
