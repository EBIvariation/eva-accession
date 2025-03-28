/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.parameters;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

public class InputParameters {

    private String vcf;

    private String remappedFrom;

    private String assemblyAccession;

    private String loadTo;

    private String assemblyReportUrl;

    private String remappingVersion;

    private int chunkSize;

    private boolean forceRestart;

    public String getVcf() {
        return vcf;
    }

    public void setVcf(String vcf) {
        this.vcf = vcf;
    }

    public String getRemappedFrom() {
        return remappedFrom;
    }

    public void setRemappedFrom(String remappedFrom) {
        this.remappedFrom = remappedFrom;
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    public String getLoadTo() {
        return loadTo;
    }

    public void setLoadTo(String loadTo) {
        this.loadTo = loadTo;
    }

    public String getAssemblyReportUrl() {
        return assemblyReportUrl;
    }

    public void setAssemblyReportUrl(String assemblyReportUrl) {
        this.assemblyReportUrl = assemblyReportUrl;
    }

    public String getRemappingVersion() {
        return remappingVersion;
    }

    public void setRemappingVersion(String remappingVersion) {
        this.remappingVersion = remappingVersion;
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

    public JobParameters toJobParameters() {
        return new JobParametersBuilder()
                .addString("remappedFrom", remappedFrom)
                .addString("assemblyAccession", assemblyAccession)
                .addString("vcf", vcf)
                .addString("loadTo", loadTo)
                .addString("assemblyReportUrl", assemblyReportUrl)
                .addString("remappingVersion", remappingVersion)
                .addLong("chunkSize", (long) chunkSize, false)
                .toJobParameters();
    }
}
