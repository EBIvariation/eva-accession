package uk.ac.ebi.eva.accession.core.io;

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
        long position = this.getPosition(fields);
        Set<String> ids = this.getIds(fields);
        String reference = this.getReference(fields);
        String[] alternateAlleles = this.getAlternateAlleles(fields);
        List<Variant> variants = new LinkedList();
        String[] var12 = alternateAlleles;
        int var13 = alternateAlleles.length;

        for (int var14 = 0; var14 < var13; ++var14) {
            String alternateAllele = var12[var14];

            VariantCoreFields keyFields;
            try {
                keyFields = getVariantCoreKeyFields(chromosome, position, reference, alternateAllele);
            } catch (NonVariantException var18) {
                continue;
            }

            Variant variant = new Variant(chromosome, keyFields.getStart(), keyFields.getEnd(), keyFields.getReference(), keyFields.getAlternate());
            variant.setIds(ids);
            if (ids.size() > 0) {
                variant.setMainId((String) ids.iterator().next());
            }

            variants.add(variant);
        }

        return variants;
    }

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
