/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.parameters;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;

import java.util.List;
import java.util.stream.Collectors;

public class InputParameters {

    private String vcf;

    private String remappedFrom;

    private String projectAccession;  // used for clustering from VCF job

    private List<String> projects;  // used for study clustering from Mongo job

    private String assemblyAccession;

    private String rsReportPath;

    private int chunkSize;

    private boolean forceRestart;

    private boolean allowRetry;

    private String rsAccFile;

    private String duplicateRSAccFile;

    public String getVcf() {
        return vcf;
    }

    public void setVcf(String vcf) {
        this.vcf = vcf;
    }

    public String getProjectAccession() {
        return projectAccession;
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public List<String> getProjects() {
        return projects;
    }

    public void setProjects(List<String> projects) {
        this.projects = projects;
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    public String getRSReportPath() {
        return rsReportPath;
    }

    public void setRSReportPath(String rsReportPath) {
        this.rsReportPath = rsReportPath;
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

    public boolean isAllowRetry() {
        return allowRetry;
    }

    public void setAllowRetry(boolean allowRetry) {
        this.allowRetry = allowRetry;
    }

    public JobParameters toJobParameters() throws JobParametersInvalidException {
        projects = projects.stream().map(String::trim).collect(Collectors.toList());
        if (projects.stream().anyMatch(s -> s.contains(","))) {
            throw new JobParametersInvalidException("Can't have commas in project accessions");
        }
        String projectsString = CollectionUtils.isEmpty(projects) ? "" : String.join(",", projects);
        if (projectsString.length() > 250) {
            throw new JobParametersInvalidException("Max length of projects parameter is 250 characters");
        }

        return new JobParametersBuilder()
                .addString("assemblyAccession", assemblyAccession)
                .addString("projectAccession", projectAccession)
                .addString("projects", projectsString)
                .addString("vcf", vcf)
                .addLong("chunkSize", (long) chunkSize, false)
                .toJobParameters();
    }

    public String getRemappedFrom() {
        return remappedFrom;
    }

    public void setRemappedFrom(String remappedFrom) {
        this.remappedFrom = remappedFrom;
    }

    public String getRsAccFile() {
        return rsAccFile;
    }

    public void setRsAccFile(String rsAccFile) {
        this.rsAccFile = rsAccFile;
    }

    public String getDuplicateRSAccFile() {
        return duplicateRSAccFile;
    }

    public void setDuplicateRSAccFile(String duplicateRSAccFile) {
        this.duplicateRSAccFile = duplicateRSAccFile;
    }
}
