package uk.ac.ebi.eva.accession.clustering.batch.listeners;

public enum Metric {
    SUBMITTED_VARIANTS("submitted_variants", "Number of variants(SS) picked up for clustering"),
    CREATED_VARIANTS("created_variants", "Number of Variants Created (RS created)"),
    UPDATED_VARIANTS("updated_variants", "Number of Variants Updated (RS updated)"),
    MERGED_VARIANTS("merged_variants", "Number of Variants Merged (RS merged)"),
    SUBMITTED_VARIANTS_CLUSTERED("submitted_variants_clustered", "Number of Variants Clustered (SS)"),
    SUBMITTED_VARIANTS_UNCLUSTERED("submitted_variants_unclustered", "Number of Variants(SS) kept unclustered"),
    SUBMITTED_VARIANTS_RS_MERGED("submitted_variants_rs_merged", "Number of Submitted Variants updated because of RS merged"),
    SUBMITTED_VARIANTS_UPDATED_OPERATIONS("submitted_variants_updated", "Number of Submitted Variants update operations performed");

    private String name;
    private String def;

    Metric(String name, String def) {
        this.name = name;
        this.def = def;
    }

    public String getName() {
        return name;
    }

    public String getDef() {
        return def;
    }

    @Override
    public String toString() {
        return name;
    }

}
