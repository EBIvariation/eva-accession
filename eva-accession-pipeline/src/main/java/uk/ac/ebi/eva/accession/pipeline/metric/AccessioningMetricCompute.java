package uk.ac.ebi.eva.accession.pipeline.metric;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.BaseMetricCompute;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AccessioningMetricCompute extends BaseMetricCompute<AccessioningMetric> {
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
            JSONObject identifier = new JSONObject();
            identifier.put("assembly", assembly);
            identifier.put("study", study);
            return identifier.toString();
        } catch (JSONException jsonException) {
            throw new RuntimeException("Could not create Identifier for Accessioning Counts. Error ", jsonException);
        }
    }

    public long getCount(AccessioningMetric metric) {
        return metric.getCount();
    }

    public void addCount(AccessioningMetric metric, long count) {
        metric.addCount(count);
    }

}
