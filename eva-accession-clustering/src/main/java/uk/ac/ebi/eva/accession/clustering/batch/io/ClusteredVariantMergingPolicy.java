package uk.ac.ebi.eva.accession.clustering.batch.io;

/**
 * This class represents the policy for choosing which RS ID to keep when 2 of them are merged.
 */
public class ClusteredVariantMergingPolicy {

    public static class Priority {
        public final Long accessionToKeep;
        public final Long accessionToBeMerged;

        public Priority(Long accessionToKeep, Long accessionToBeMerged) {
            this.accessionToKeep = accessionToKeep;
            this.accessionToBeMerged = accessionToBeMerged;
        }
    }

    /**
     * At the moment, the priority is just to keep the oldest accession, but other policies might apply, like choosing
     * the accession with clinical relevance (which we don't store at the moment of writing).
     *
     * @see <a href="https://ncbijira.ncbi.nlm.nih.gov/secure/attachment/95534/95534_VAR-RSIDAssignment-100220-1039-34.pdf">
     *     dbSNP policies for assigning RS IDs</a>
     */
    public static Priority prioritise(Long oneAccession, Long anotherAccession) {
        if (oneAccession < anotherAccession) {
            return new Priority(oneAccession, anotherAccession);
        } else {
            return new Priority(anotherAccession, oneAccession);
        }
    }

}
