package uk.ac.ebi.eva.accession.core.repositoryHuman;

import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRange;
import uk.ac.ebi.ampt2d.commons.accession.service.BasicSpringDataRepositoryMonotonicDatabaseService;

import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.service.DbsnpClusteredVariantInactiveService;

import java.util.Collection;

public class DbsnpClusteredHumanVariantAccessioningDatabaseService
        extends BasicSpringDataRepositoryMonotonicDatabaseService<IClusteredVariant, DbsnpClusteredVariantEntity> {

    public DbsnpClusteredHumanVariantAccessioningDatabaseService(DbsnpHumanClusteredVariantAccessionRepository repository,
                                                                 DbsnpClusteredVariantInactiveService inactiveService) {
        super(repository,
              accessionWrapper -> new DbsnpClusteredVariantEntity(accessionWrapper.getAccession(),
                                                                  accessionWrapper.getHash(),
                                                                  accessionWrapper.getData(),
                                                                  accessionWrapper.getVersion()),
              inactiveService);
    }

    @Override
    public long[] getAccessionsInRanges(Collection<MonotonicRange> ranges) {
        throw new UnsupportedOperationException("New accessions cannot be issued for dbSNP variants");
    }
}
