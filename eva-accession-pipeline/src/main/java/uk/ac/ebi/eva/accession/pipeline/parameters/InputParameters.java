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

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import uk.ac.ebi.eva.commons.core.models.Aggregation;

public class InputParameters {

    private String vcf;

    private Aggregation vcfAggregation;

    private String aggregatedMappingFile;

    private String fasta;

    private String outputVcf;

    private String assemblyAccession;

    private int taxonomyAccession;

    private String projectAccession;

    private int chunkSize;

    private boolean forceRestart;

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

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    public void setTaxonomyAccession(int taxonomyAccession) {
        this.taxonomyAccession = taxonomyAccession;
    }

    public String getProjectAccession() {
        return projectAccession;
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public JobParameters toJobParameters() {
        return new JobParametersBuilder()
                .addString("projectAccession", projectAccession)
                .addString("vcf", vcf)
                .addString("vcfAggregation", vcfAggregation.toString())
                .addString("aggregatedMappingFile", aggregatedMappingFile)
                .addString("fasta", fasta)
                .addString("outputVcf", outputVcf)
                .addLong("taxonomyAccession", (long)taxonomyAccession)
                .addString("projectAccession", projectAccession)
                .addLong("chunkSize", (long)chunkSize)
                .toJobParameters();
    }

    public boolean isForceRestart() {
        return forceRestart;
    }

    public void setForceRestart(boolean forceRestart) {
        this.forceRestart = forceRestart;
    }
}
