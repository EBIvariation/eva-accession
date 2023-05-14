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

package uk.ac.ebi.eva.accession.release.parameters;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

import uk.ac.ebi.eva.accession.core.contig.ContigNaming;

public class InputParameters {

    private String assemblyAccession;

    private int taxonomyAccession;

    private String fasta;

    private String assemblyReportUrl;

    private String outputFolder;

    private boolean forceRestart;

    private int chunkSize;

    private String accessionedVcf;

    private ContigNaming contigNaming;

    public JobParameters toJobParameters() {
        return new JobParametersBuilder()
                .addString("assemblyAccession", assemblyAccession)
                .addLong("taxonomyAccession", (long) taxonomyAccession)
                .addString("fasta", fasta)
                .addString("assemblyReportUrl", assemblyReportUrl)
                .addString("outputFolder", outputFolder)
                .toJobParameters();
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

    public String getFasta() {
        return fasta;
    }

    public void setFasta(String fasta) {
        this.fasta = fasta;
    }

    public String getAssemblyReportUrl() {
        return assemblyReportUrl;
    }

    public void setAssemblyReportUrl(String assemblyReportUrl) {
        this.assemblyReportUrl = assemblyReportUrl;
    }

    public String getOutputFolder() {
        return outputFolder;
    }

    public void setOutputFolder(String outputFolder) {
        this.outputFolder = outputFolder;
    }

    public boolean isForceRestart() {
        return forceRestart;
    }

    public void setForceRestart(boolean forceRestart) {
        this.forceRestart = forceRestart;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public String getAccessionedVcf() {
        return accessionedVcf;
    }

    public void setAccessionedVcf(String accessionedVcf) {
        this.accessionedVcf = accessionedVcf;
    }

    public ContigNaming getContigNaming() {
        return contigNaming;
    }

    public void setContigNaming(ContigNaming contigNaming) {
        this.contigNaming = contigNaming;
    }
}
