/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.parameters;

public class InputParameters {

    private int chunkSize;

    private String assemblyAccession;

    private String projectAccession;

    private String deprecationIdSuffix;

    private String deprecationReason;

    private String variantIdFile;

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public String getAssemblyAccession() {
        return assemblyAccession;
    }

    public void setAssemblyAccession(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
    }

    public String getProjectAccession() {
        return projectAccession;
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public String getDeprecationIdSuffix() {
        return deprecationIdSuffix;
    }

    public void setDeprecationIdSuffix(String deprecationIdSuffix) {
        this.deprecationIdSuffix = deprecationIdSuffix;
    }

    public String getDeprecationReason() {
        return deprecationReason;
    }

    public void setDeprecationReason(String deprecationReason) {
        this.deprecationReason = deprecationReason;
    }

    public String getVariantIdFile() {
        return variantIdFile;
    }

    public void setVariantIdFile(String variantIdFile) {
        this.variantIdFile = variantIdFile;
    }
}
