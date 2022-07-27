package uk.ac.ebi.eva.accession.clustering.metric;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.BaseMetricCompute;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClusteringMetricCompute extends BaseMetricCompute<ClusteringMetric> {
    private static final String PROCESS = "clustering";
    private final String assembly;

    private List<String> projects;

    public ClusteringMetricCompute(CountServiceParameters countServiceParameters, RestTemplate restTemplate,
                                   String assembly, List<String> projects) {
        super(countServiceParameters, restTemplate);
        this.assembly = assembly;
        this.projects = projects;
    }

    public String getProcessName() {
        return PROCESS;
    }

    public List<ClusteringMetric> getMetrics() {
        return Arrays.stream(ClusteringMetric.values()).collect(Collectors.toList());
    }

    public String getIdentifier() {
        try {
            JSONObject identifier = new JSONObject();
            identifier.put("assembly", assembly);
            identifier.put("projects", projects);
            return identifier.toString();
        } catch (JSONException jsonException) {
            throw new RuntimeException("Could not create Identifier for Clustering Counts. Error ", jsonException);
        }
    }

    public long getCount(ClusteringMetric metric) {
        return metric.getCount();
    }

    public void addCount(ClusteringMetric metric, long count) {
        metric.addCount(count);
    }

}
