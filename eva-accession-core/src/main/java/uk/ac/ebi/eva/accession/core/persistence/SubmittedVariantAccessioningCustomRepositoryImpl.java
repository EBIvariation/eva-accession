package uk.ac.ebi.eva.accession.core.persistence;

import org.springframework.stereotype.Repository;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.accession.repositories.BasicJpaAccessionedObjectCustomRepositoryImpl;

import javax.persistence.EntityManager;

@Repository
public class SubmittedVariantAccessioningCustomRepositoryImpl extends BasicJpaAccessionedObjectCustomRepositoryImpl {

    public SubmittedVariantAccessioningCustomRepositoryImpl(EntityManager entityManager) {
        super(SubmittedVariantEntity.class, entityManager);
    }
}
