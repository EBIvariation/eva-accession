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
package uk.ac.ebi.eva.accession.dbsnp.persistence;

import org.springframework.data.mongodb.core.mapping.Document;

@Document
public class ProjectAccessionMapping {

    private String evaStudyId;

    private String dbsnpBatchHandle;

    private String dbsnpBatchName;

    private int taxonomyAccession;

    ProjectAccessionMapping() {
    }

    public ProjectAccessionMapping(String evaStudyId, String dbsnpBatchHandle, String dbsnpBatchName,
                                   int taxonomyAccession) {
        this.evaStudyId = evaStudyId;
        this.dbsnpBatchHandle = dbsnpBatchHandle;
        this.dbsnpBatchName = dbsnpBatchName;
        this.taxonomyAccession = taxonomyAccession;
    }

    public String getEvaStudyId() {
        return evaStudyId;
    }

    public void setEvaStudyId(String evaStudyId) {
        this.evaStudyId = evaStudyId;
    }

    public String getDbsnpBatchHandle() {
        return dbsnpBatchHandle;
    }

    public void setDbsnpBatchHandle(String dbsnpBatchHandle) {
        this.dbsnpBatchHandle = dbsnpBatchHandle;
    }

    public String getDbsnpBatchName() {
        return dbsnpBatchName;
    }

    public void setDbsnpBatchName(String dbsnpBatchName) {
        this.dbsnpBatchName = dbsnpBatchName;
    }

    public int getTaxonomyAccession() {
        return taxonomyAccession;
    }

    public void setTaxonomyAccession(int taxonomyAccession) {
        this.taxonomyAccession = taxonomyAccession;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ProjectAccessionMapping)) {
            return false;
        }

        ProjectAccessionMapping that = (ProjectAccessionMapping) o;

        if (taxonomyAccession != that.taxonomyAccession) {
            return false;
        }
        if (evaStudyId != null ? !evaStudyId.equals(that.evaStudyId) : that.evaStudyId != null) {
            return false;
        }
        if (dbsnpBatchHandle != null ? !dbsnpBatchHandle
                .equals(that.dbsnpBatchHandle) : that.dbsnpBatchHandle != null) {
            return false;
        }
        return dbsnpBatchName != null ? dbsnpBatchName.equals(that.dbsnpBatchName) : that.dbsnpBatchName == null;
    }

    @Override
    public int hashCode() {
        int result = evaStudyId != null ? evaStudyId.hashCode() : 0;
        result = 31 * result + (dbsnpBatchHandle != null ? dbsnpBatchHandle.hashCode() : 0);
        result = 31 * result + (dbsnpBatchName != null ? dbsnpBatchName.hashCode() : 0);
        result = 31 * result + taxonomyAccession;
        return result;
    }
}
