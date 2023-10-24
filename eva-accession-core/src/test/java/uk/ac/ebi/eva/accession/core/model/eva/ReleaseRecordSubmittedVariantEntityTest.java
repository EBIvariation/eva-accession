package uk.ac.ebi.eva.accession.core.model.eva;

import org.junit.Test;
import uk.ac.ebi.eva.accession.core.model.ReleaseRecordSubmittedVariantEntity;

import static org.junit.Assert.assertEquals;

public class ReleaseRecordSubmittedVariantEntityTest {

    @Test
    public void testChangeSVERefAltToUpperCase() {
        ReleaseRecordSubmittedVariantEntity releaseRecordSubmittedVariantEntity =
                new ReleaseRecordSubmittedVariantEntity(123l, "hashedMessage",
                        "PRJEB12345", "chr", 1, "a", "t",
                        "a", "t", false,
                        false, false, true);

        assertEquals("A", releaseRecordSubmittedVariantEntity.getReferenceAllele());
        assertEquals("T", releaseRecordSubmittedVariantEntity.getAlternateAllele());
    }

}
