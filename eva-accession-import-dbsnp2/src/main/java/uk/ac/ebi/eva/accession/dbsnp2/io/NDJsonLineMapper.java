package uk.ac.ebi.eva.accession.dbsnp2.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.LineMapper;

public class NDJsonLineMapper implements LineMapper<JsonNode> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode mapLine(String line, int lineNumber) throws Exception {
        return objectMapper.readValue(line, JsonNode.class);
    }
}
