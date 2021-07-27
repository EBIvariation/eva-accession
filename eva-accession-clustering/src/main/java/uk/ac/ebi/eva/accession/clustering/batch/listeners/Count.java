package uk.ac.ebi.eva.accession.clustering.batch.listeners;

public class Count {
    private Long id;
    private String process;
    private String identifier;
    private String metric;
    private long count;

    public Count(String process, String identifier, String metric, long count) {
        this.process = process;
        this.identifier = identifier;
        this.metric = metric;
        this.count = count;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProcess() {
        return process;
    }

    public void setProcess(String process) {
        this.process = process;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "Count{" +
                "id=" + id +
                ", process='" + process + '\'' +
                ", identifier='" + identifier + '\'' +
                ", metric='" + metric + '\'' +
                ", count=" + count +
                '}';
    }
}
