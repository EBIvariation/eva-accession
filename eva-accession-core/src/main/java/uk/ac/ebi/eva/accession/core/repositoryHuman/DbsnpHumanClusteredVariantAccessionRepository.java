package uk.ac.ebi.eva.accession.core.repositoryHuman;

import org.springframework.stereotype.Repository;
import uk.ac.ebi.ampt2d.commons.accession.persistence.repositories.IAccessionedObjectRepository;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

@Repository
public interface DbsnpHumanClusteredVariantAccessionRepository extends
        IAccessionedObjectRepository<DbsnpClusteredVariantEntity, Long> {
}
