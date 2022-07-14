package uk.ac.ebi.eva.accession.clustering.metric;

import uk.ac.ebi.eva.metrics.metric.Metric;

public enum ClusteringMetric implements Metric {

    CLUSTERED_VARIANTS_CREATED("clustered_variants_created", "Number of new clustered variants created", 0),
    CLUSTERED_VARIANTS_UPDATED("clustered_variants_updated","Number of clustered variants updated",0),
    CLUSTERED_VARIANTS_RS_SPLIT("clustered_variants_rs_split", "Number of clustered variants whose rs is split", 0),
    CLUSTERED_VARIANTS_MERGED("clustered_variants_merged", "Number of clustered variants merged", 0),
    CLUSTERED_VARIANTS_MERGE_OPERATIONS("clustered_variants_merge_operations", "Number of merge operations on clustered variants", 0),
    CLUSTERED_VARIANTS_DEPRECATED("clustered_variants_deprecated", "Number of clustered variants that have been deprecated", 0),
    SUBMITTED_VARIANTS_CLUSTERED("submitted_variants_clustered", "Number of Variants(SS) Clustered", 0),
    SUBMITTED_VARIANTS_KEPT_UNCLUSTERED("submitted_variants_kept_unclustered", "Number of submitted variants kept unclustered", 0),
    SUBMITTED_VARIANTS_UPDATED_RS("submitted_variants_updated_rs", "Number of variants(ss) whose rs is updated", 0),
    SUBMITTED_VARIANTS_UPDATE_OPERATIONS("submitted_variants_update_operations", "Number of submitted variants update operations performed", 0),
    SUBMITTED_VARIANTS_SS_SPLIT("submitted_variants_ss_split", "Number of submitted variants whose ss is split", 0),
    SUBMITTED_VARIANTS_DEPRECATED("submitted_variants_deprecated", "Number of submitted variants that have been deprecated", 0);

    private String name;
    private String description;
    private long count;

    ClusteringMetric(String name, String description, long count) {
        this.name = name;
        this.description = description;
        this.count = count;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public long getCount() {
        return this.count;
    }

    public void addCount(long count) {
        this.count += count;
    }

    public void clearCount() {
        this.count = 0;
    }

    @Override
    public String toString() {
        return name;
    }
}