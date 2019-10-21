package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.DatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

public class ClusteredHumanVariantAccessioningService extends BasicAccessioningService<IClusteredVariant, String, Long> {

    public ClusteredHumanVariantAccessioningService(DbsnpMonotonicAccessionGenerator<IClusteredVariant> generator,
                                                    DatabaseService<IClusteredVariant, String, Long> dbServiceDbsnp) {
        super(generator, dbServiceDbsnp, new ClusteredVariantSummaryFunction(), new SHA1HashingFunction());
    }

}
