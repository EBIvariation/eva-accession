/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.dbsnp.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpJson;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;

import java.io.File;
import java.io.IOException;

/**
 * Item reader for Dbsnp Json import job
 */
public class DbsnpJsonItemReader implements ItemReader<DbsnpJson> {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpJsonItemReader.class);

    private InputParameters parameters;

    public DbsnpJsonItemReader(InputParameters parameters) {
        this.parameters = parameters;
    }

    @Override
    public DbsnpJson read() throws Exception {
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jp = jsonFactory.createJsonParser(new File(parameters.getDbsnp()));
        jp.setCodec(new ObjectMapper());
        JsonNode jsonNode = jp.readValueAsTree();
        return parseDbsnpJsonPlacements(jsonNode);
    }

    /**
     * Parses the JSON as per https://github.com/ncbi/dbsnp/blob/master/tutorials/hadoop_json_placement.py
     * @param JsonNode jsonRootNode
     * @return parsed DbsnpJson object
     */
    private DbsnpJson parseDbsnpJsonPlacements(JsonNode jsonRootNode) throws IOException {

        DbsnpJson dbsnpJson = new DbsnpJson();
        dbsnpJson.setClusteredVariantAccession(jsonRootNode.path("refsnp_id").asLong());

        //TODO for now skip handling of case when primary_snapshot_data isn't present
        //TODO decide for that case between an exception with get(), or with an if size = 0 conditional with path()
        JsonNode infoNode = jsonRootNode.path("primary_snapshot_data").path("placements_with_allele");

        for(JsonNode alleleInfo : infoNode) {

            boolean isPtlp = alleleInfo.path("is_ptlp").asBoolean();
            JsonNode assemblyInfo = alleleInfo.path("placement_annot").path("seq_id_traits_by_assembly");
            if(assemblyInfo.size() == 0 || !isPtlp) {
                continue;
            }

            dbsnpJson.setProjectAccession(assemblyInfo.get(0).path("assembly_name").asText());

            for (JsonNode alleleAndHgvs : alleleInfo.path("alleles")) {
                JsonNode spdi = alleleAndHgvs.path("allele").path("spdi");
                String insertedSequence = spdi.path("inserted_sequence").asText();
                String deletedSequence = spdi.path("deleted_sequence").asText();
                if(!insertedSequence.equals(deletedSequence)) {
                    dbsnpJson.setReferenceAllele(deletedSequence);
                    dbsnpJson.setAlternateAllele(insertedSequence);
                    dbsnpJson.setStart(spdi.path("position").asLong());
                    dbsnpJson.setContig(spdi.path("seq_id").asText());
                }
            }
        }

        //TODO remove by 01/07/2019
        logger.info("DbsnpJson: {}", new ObjectMapper().writeValueAsString(dbsnpJson));
        return dbsnpJson;
    }
}
