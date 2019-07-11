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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.ClusteredVariant;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;

public class JsonNodeToClusteredVariantProcessor implements ItemProcessor<JsonNode, DbsnpClusteredVariantEntity> {

    private static Logger logger = LoggerFactory.getLogger(JsonNodeToClusteredVariantProcessor.class);
    private Function<IClusteredVariant, String> hashingFunction =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public JsonNodeToClusteredVariantProcessor() {
    }

    /**
     * Parses the JSON into a DbsnpClusteredVariantEntity
     * @param jsonRootNode root node of a dbSNP JSON line item
     * @return dbsnpClusteredVariantEntity
     */
    public DbsnpClusteredVariantEntity process(JsonNode jsonRootNode) {
        long accession = jsonRootNode.path("refsnp_id").asLong();
        ClusteredVariant clusteredVariant = parseJsonNodeToClusteredVariant(jsonRootNode);
        String hashedMessage = hashingFunction.apply(clusteredVariant);
        return new DbsnpClusteredVariantEntity(accession, hashedMessage, clusteredVariant);
    }

    private ClusteredVariant parseJsonNodeToClusteredVariant(JsonNode jsonRootNode) {
        // @see <a href=https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=9606>Human Tax ID.</a>
        int taxonomyAccession = 9606;

        // JSON date in ISO-8601 format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-d'T'HH:mm'Z'");
        LocalDateTime createdDate = LocalDateTime.parse(jsonRootNode.path("create_date").asText(), formatter);
        VariantType type = prepareVariantType(jsonRootNode.path("primary_snapshot_data").path("variant_type").asText());
        JsonNode infoNode = jsonRootNode.path("primary_snapshot_data").path("placements_with_allele");
        for(JsonNode alleleInfo : infoNode) {
            String assemblyAccession;
            boolean isPtlp = alleleInfo.path("is_ptlp").asBoolean();
            JsonNode assemblyInfo = alleleInfo.path("placement_annot").path("seq_id_traits_by_assembly");
            if(assemblyInfo.size() == 0 || !isPtlp) {
                continue;
            }
            assemblyAccession = assemblyInfo.get(0).path("assembly_accession").asText();
            String contig, alternateAllele, referenceAllele;
            long start = 0L;
            for (JsonNode alleleAndHgvs : alleleInfo.path("alleles")) {
                JsonNode spdi = alleleAndHgvs.path("allele").path("spdi");
                alternateAllele = spdi.path("inserted_sequence").asText();
                referenceAllele = spdi.path("deleted_sequence").asText();
                if(alternateAllele.equals(referenceAllele)) {
                    contig = spdi.path("seq_id").asText();
                    start = spdi.path("position").asLong(); //dbSNP stores in 0 base, EVA in 1 base
                    return new ClusteredVariant(assemblyAccession, taxonomyAccession, contig, start,
                            type, Boolean.FALSE, createdDate);
                }
            }
        }
        // absence of primary top level placement (PLTP) data
        logger.error("Primary top level placement data not present for accession refSNP ID: {}",
            jsonRootNode.path("refsnp_id").asLong());
        return null;
    }

    /**
     * @see <a href=https://api.ncbi.nlm.nih.gov/variation/v0/#/>dbSNP JSON 2.0 schema spec</a>
     * @param variantType DbSNP2.0 JSON variant type
     * @return EVA variant type representation
     */
    private VariantType prepareVariantType(String variantType) {
        switch(variantType.toUpperCase()) {
            case "SNV":
                return VariantType.SNV;
            case "MNV":
                return VariantType.MNV;
            case "INS":
                return VariantType.INS;
            case "DEL":
                return VariantType.DEL;
            case "DELINS":
                return VariantType.INDEL;
            case "IDENTITY":
                return VariantType.NO_SEQUENCE_ALTERATION;
            default:
                throw new IllegalArgumentException(
                    "The dbSNP variant type provided doesn't have a direct mapping to an EVA type");
        }
    }
}
