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
package uk.ac.ebi.eva.accession.dbsnp2.parameters;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

public class InputParameters {

    private String input;
    private String genbankAssembly;
    private String refseqAssembly;
    private String assemblyReportUrl;
    private int chunkSize;
    private boolean forceRestart;
    private boolean forceImport;

    public JobParameters toJobParameters() {
        return new JobParametersBuilder()
            .addString("input", input)
            .addString("genbankAssembly", genbankAssembly)
            .addString("refseqAssembly", refseqAssembly)
            .addLong("chunkSize", (long) chunkSize, false)
            .toJobParameters();
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getGenbankAssembly() {
        return genbankAssembly;
    }

    public void setGenbankAssembly(String genbankAssembly) {
        this.genbankAssembly = genbankAssembly;
    }

    public String getRefseqAssembly() {
        return refseqAssembly;
    }

    public void setRefseqAssembly(String refseqAssembly) {
        this.refseqAssembly = refseqAssembly;
    }

    public String getAssemblyReportUrl() {
        return assemblyReportUrl;
    }

    public void setAssemblyReportUrl(String assemblyReportUrl) {
        this.assemblyReportUrl = assemblyReportUrl;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public boolean isForceRestart() {
        return forceRestart;
    }

    public void setForceRestart(boolean forceRestart) {
        this.forceRestart = forceRestart;
    }

    public boolean isForceImport() {
        return forceImport;
    }

    public void setForceImport(boolean forceImport) {
        this.forceImport = forceImport;
    }
}
