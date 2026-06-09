package uk.ac.ebi.eva.accession.clustering.metric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.BaseMetricCompute;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClusteringMetricCompute extends BaseMetricCompute<ClusteringMetric> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
            ObjectNode identifier = OBJECT_MAPPER.createObjectNode();
            identifier.put("assembly", assembly);
            identifier.putPOJO("projects", projects);
            return OBJECT_MAPPER.writeValueAsString(identifier);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Could not create Identifier for Clustering Counts. Error ", e);
        }
    }

    public long getCount(ClusteringMetric metric) {
        return metric.getCount();
    }

    public void addCount(ClusteringMetric metric, long count) {
        metric.addCount(count);
    }

}
