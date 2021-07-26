package uk.ac.ebi.eva.accession.core.batch.io;

import uk.ac.ebi.eva.commons.core.models.VariantCoreFields;
import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AccessionedVcfFactory extends VariantVcfFactory {

    public AccessionedVcfFactory() {
        this.includeIds = true;
    }

    public List<Variant> create(String fileId, String studyId, String line) throws IllegalArgumentException, NonVariantException, IncompleteInformationException {
        String[] fields = line.split("\t", 6);
        String chromosome = fields[0];
        long position = getPosition(fields);
        Set<String> ids = getIds(fields);
        String reference = getReference(fields);
        String[] alternateAlleles = getAlternateAlleles(fields);

        List<Variant> variants = new LinkedList<>();
        for (String alternateAllele : alternateAlleles) {
            VariantCoreFields keyFields;
            try {
                keyFields = getVariantCoreKeyFields(chromosome, position, reference, alternateAllele);
            } catch (NonVariantException e) {
                continue;
            }

            Variant variant = new Variant(chromosome, keyFields.getStart(), keyFields.getEnd(), keyFields.getReference(), keyFields.getAlternate());
            variant.setIds(ids);
            if (ids.size() > 0) {
                variant.setMainId(ids.iterator().next());
            }

            variants.add(variant);
        }

        return variants;
    }

    /**
     * @param chromosome
     * @param position
     * @param reference
     * @param alternateAllele
     * @return VariantCoreFields
     * When reading variant from Accessioned VCF, this method checks if a context base has been added to the Variant.
     * If yes, we need to remove that first, in order to make the representation consistent and then give to VariantCoreFields
     * for other checks
     *
     * ex: Assume the following variant   ->     After right trimming    ->     stored in vcf
     * CHR POS  REF  ALT                         CHR POS REF ALT                CHR POS REF  ALT
     * 1   100  CAGT  T                          1  100 CAG                     1  99  GCAG  G
     *
     * Storing in VCF (as per normalition algorithm, VCF cannot store an empty REF or ALT. If after right trimming REF or ALT become empty,
     * a context base needs to be added)
     *
     * reading without context base adjustment                  reading with context base adjustment
     * CHR POS REF ALT                                          CHR POS REF ALT
     * 1   99  GCA                                              1   100 CAG
     */
    private VariantCoreFields getVariantCoreKeyFields(String chromosome, long position, String reference, String alternateAllele) {
        if (isContextBasePresent(reference, alternateAllele)) {
            if (alternateAllele.length() == 1) {
                alternateAllele = "";
                reference = reference.substring(1);
            } else if (reference.length() == 1) {
                reference = "";
                alternateAllele = alternateAllele.substring(1);
            }
            position = position + 1;
        }
        return new VariantCoreFields(chromosome, position, reference, alternateAllele);
    }

    private boolean isContextBasePresent(String reference, String alternate) {
        if (alternate.length() == 1 && reference.length() > 1 && reference.startsWith(alternate)) {
            return true;
        } else if (reference.length() == 1 && alternate.length() > 1 && alternate.startsWith(reference)) {
            return true;
        } else {
            return false;
        }
    }

    protected void parseSplitSampleData(VariantSourceEntry variantSourceEntry, String[] strings, int i) {
        throw new UnsupportedOperationException("This factory doesn't support sample parsing");
    }
}
