/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp2.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.item.file.LineMapper;

/**
 * Maps a line in a new-line delimited (ND)JSON file to a JsonNode.
 */
public class JsonNodeLineMapper implements LineMapper<JsonNode> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public JsonNode mapLine(String line, int lineNumber) throws Exception {
        return objectMapper.readValue(line, JsonNode.class);
    }
}
