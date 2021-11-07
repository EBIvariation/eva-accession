package uk.ac.ebi.eva.accession.pipeline.metric;

import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.BaseMetricCompute;
import uk.ac.ebi.eva.metrics.metric.Metric;
import uk.ac.ebi.eva.metrics.util.MetricUtil;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AccessioningMetricCompute extends BaseMetricCompute {
    private static final String PROCESS = "accessioning_warehouse_ingestion";
    private final String assembly;
    private final String study;

    public AccessioningMetricCompute(CountServiceParameters countServiceParameters, RestTemplate restTemplate,
                                     String assembly, String study) {
        super(countServiceParameters, restTemplate);
        this.assembly = assembly;
        this.study = study;
    }

    public String getProcessName() {
        return PROCESS;
    }

    public List<Metric> getMetrics() {
        return Arrays.stream(AccessioningMetric.values()).collect(Collectors.toList());
    }

    public String getIdentifier() {
        return MetricUtil.createAccessionIdentifier(this.assembly, this.study);
    }

    public long getCount(Metric metric) {
        return metric.getCount();
    }

    public void addCount(Metric metric, long count) {
        metric.addCount(count);
    }

}
