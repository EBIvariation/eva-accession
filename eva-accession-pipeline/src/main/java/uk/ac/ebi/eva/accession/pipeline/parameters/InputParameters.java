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
package uk.ac.ebi.eva.accession.pipeline.parameters;

import uk.ac.ebi.eva.commons.core.models.Aggregation;

import java.io.File;

public class InputParameters {

    private String vcfId;

    private String studyId;

    private String vcf;

    private Aggregation vcfAggregation;

    private String aggregatedMappingFile;

    private String fasta;

    private String outputVcf;

    /**
     * TODO: discuss if this should be a parameter or can be hardcoded. The VcfReader needs it, but we won't use this
     * field from the resulting variants.
     */
    public String getVcfId() {
        return vcfId;
    }

    public void setVcfId(String vcfId) {
        this.vcfId = vcfId;
    }

    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    public String getVcf() {
        return vcf;
    }

    public void setVcf(String vcf) {
        this.vcf = vcf;
    }

    public Aggregation getVcfAggregation() {
        return vcfAggregation;
    }

    public void setVcfAggregation(Aggregation vcfAggregation) {
        this.vcfAggregation = vcfAggregation;
    }

    public String getAggregatedMappingFile() {
        return aggregatedMappingFile;
    }

    public void setAggregatedMappingFile(String aggregatedMappingFile) {
        this.aggregatedMappingFile = aggregatedMappingFile;
    }

    public String getFasta() {
        return fasta;
    }

    public void setFasta(String fasta) {
        this.fasta = fasta;
    }

    public String getOutputVcf() {
        return outputVcf;
    }

    public void setOutputVcf(String outputVcf) {
        this.outputVcf = outputVcf;
    }
}
