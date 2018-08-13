/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.listeners;

public class ImportCounts {

    public static final String SUBMITTED_VARIANTS_WRITTEN = "submittedVariantsWritten";

    public static final String CLUSTERED_VARIANTS_WRITTEN = "clusteredVariantsWritten";

    public static final String OPERATIONS_WRITTEN = "operationsWritten";

    private long clusteredVariantsWritten;

    private long operationsWritten;

    private long submittedVariantsWritten;

    public ImportCounts() {
        this.clusteredVariantsWritten = 0;
        this.submittedVariantsWritten = 0;
        this.operationsWritten = 0;
    }

    public void addClusteredVariantsWritten(long clusteredVariantsWritten) {
        this.clusteredVariantsWritten += clusteredVariantsWritten;
    }

    public void addOperationsWritten(long operationsWritten) {
        this.operationsWritten += operationsWritten;
    }

    public void addSubmittedVariantsWritten(long submittedVariantsWritten) {
        this.submittedVariantsWritten += submittedVariantsWritten;
    }

    public long getClusteredVariantsWritten() {
        return clusteredVariantsWritten;
    }

    public void setClusteredVariantsWritten(long clusteredVariantsWritten) {
        this.clusteredVariantsWritten = clusteredVariantsWritten;
    }

    public long getOperationsWritten() {
        return operationsWritten;
    }

    public void setOperationsWritten(long operationsWritten) {
        this.operationsWritten = operationsWritten;
    }

    public long getSubmittedVariantsWritten() {
        return submittedVariantsWritten;
    }

    public void setSubmittedVariantsWritten(long submittedVariantsWritten) {
        this.submittedVariantsWritten = submittedVariantsWritten;
    }
}
