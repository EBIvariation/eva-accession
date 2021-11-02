package uk.ac.ebi.eva.accession.pipeline.metric;

import uk.ac.ebi.eva.metrics.metric.Metric;

public enum AccessioningMetric implements Metric {
    SUBMITTED_VARIANTS("submitted_variants", "Number of variants submitted for accessioning", 0),
    DISCARDED_VARIANTS("discarded_variants", "Number of variants discarded while accessioning", 0);

    private String name;
    private String description;
    private long count;

    AccessioningMetric(String name, String description, long count) {
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

    public void clearCount(){
        this.count = 0;
    }

    @Override
    public String toString() {
        return name;
    }
}