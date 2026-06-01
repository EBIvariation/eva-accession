package uk.ac.ebi.eva.accession.pipeline.metric;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.BaseMetricCompute;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AccessioningMetricCompute extends BaseMetricCompute<AccessioningMetric> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

    public List<AccessioningMetric> getMetrics() {
        return Arrays.stream(AccessioningMetric.values()).collect(Collectors.toList());
    }

    public String getIdentifier() {
        try {
            ObjectNode identifier = OBJECT_MAPPER.createObjectNode();
            identifier.put("assembly", assembly);
            identifier.put("study", study);

            return OBJECT_MAPPER.writeValueAsString(identifier);
        } catch (JsonProcessingException ex) {
            throw new RuntimeException("Could not create Identifier for Accessioning Counts. Error ", ex);
        }
    }

    public long getCount(AccessioningMetric metric) {
        return metric.getCount();
    }

    public void addCount(AccessioningMetric metric, long count) {
        metric.addCount(count);
    }

}
