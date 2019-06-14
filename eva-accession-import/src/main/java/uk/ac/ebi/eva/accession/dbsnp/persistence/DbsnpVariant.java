package uk.ac.ebi.eva.accession.dbsnp.persistence;

import uk.ac.ebi.eva.commons.core.models.AbstractVariant;
import uk.ac.ebi.eva.commons.core.models.IVariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.VariantStatistics;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DbsnpVariant extends AbstractVariant {

    private int taxonomyAccession = 9606;
    private List<IVariantSourceEntry> variantSourceEntries;

    public DbsnpVariant(String chromosome, long start, long end, String reference, String alternate) {
        super(chromosome, start, end, reference, alternate, null);
        variantSourceEntries = Collections.singletonList(
            new VariantSourceEntry(null, null, null, null,
                null, null, null));
    }

    @Override
    public Collection<? extends IVariantSourceEntry> getSourceEntries() {
        return variantSourceEntries;
    }

    @Override
    public IVariantSourceEntry getSourceEntry(String s, String s1) {
        return variantSourceEntries.get(0);
    }
}
