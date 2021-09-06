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
package uk.ac.ebi.eva.accession.clustering.batch.listeners;

public class ClusteringCounts {

    private long clusteredVariantsCreated;

    private long clusteredVariantsUpdated;

    private long clusteredVariantsMergeOperationsWritten;

    private long clusteredVariantsRSSplit;

    private long submittedVariantsClustered;

    private long submittedVariantsKeptUnclustered;

    private long submittedVariantsUpdatedRs;

    private long submittedVariantsUpdateOperationWritten;

    public ClusteringCounts() {
        this.clusteredVariantsCreated = 0;
        this.clusteredVariantsUpdated = 0;
        this.clusteredVariantsMergeOperationsWritten = 0;
        this.clusteredVariantsRSSplit = 0;
        this.submittedVariantsKeptUnclustered = 0;
        this.submittedVariantsClustered = 0;
        this.submittedVariantsUpdatedRs = 0;
        this.submittedVariantsUpdateOperationWritten = 0;
    }

    public void addClusteredVariantsCreated(long clusteredVariantsCreated) {
        this.clusteredVariantsCreated += clusteredVariantsCreated;
    }

    public long getClusteredVariantsCreated() {
        return clusteredVariantsCreated;
    }

    public void setClusteredVariantsCreated(long clusteredVariantsCreated) {
        this.clusteredVariantsCreated = clusteredVariantsCreated;
    }

    public void addClusteredVariantsUpdated(long clusteredVariantsUpdated) {
        this.clusteredVariantsUpdated += clusteredVariantsUpdated;
    }

    public long getClusteredVariantsUpdated() {
        return clusteredVariantsUpdated;
    }

    public void setClusteredVariantsUpdated(long clusteredVariantsUpdated) {
        this.clusteredVariantsUpdated = clusteredVariantsUpdated;
    }

    public void addClusteredVariantsMergeOperationsWritten(long clusteredVariantsMergeOperationsWritten) {
        this.clusteredVariantsMergeOperationsWritten += clusteredVariantsMergeOperationsWritten;
    }

    public long getClusteredVariantsMergeOperationsWritten() {
        return clusteredVariantsMergeOperationsWritten;
    }

    public void setClusteredVariantsMergeOperationsWritten(long clusteredVariantsMergeOperationsWritten) {
        this.clusteredVariantsMergeOperationsWritten = clusteredVariantsMergeOperationsWritten;
    }

    public void addClusteredVariantsRSSplit(long clusteredVariantsRSSplit) {
        this.clusteredVariantsRSSplit += clusteredVariantsRSSplit;
    }

    public long getClusteredVariantsRSSplit() {
        return clusteredVariantsRSSplit;
    }

    public void setClusteredVariantsRSSplit(long clusteredVariantsRSSplit) {
        this.clusteredVariantsRSSplit = clusteredVariantsRSSplit;
    }

    public void addSubmittedVariantsKeptUnclustered(long submittedVariantsKeptUnclustered) {
        this.submittedVariantsKeptUnclustered += submittedVariantsKeptUnclustered;
    }

    public long getSubmittedVariantsKeptUnclustered() {
        return submittedVariantsKeptUnclustered;
    }

    public void setSubmittedVariantsKeptUnclustered(long submittedVariantsKeptUnclustered) {
        this.submittedVariantsKeptUnclustered = submittedVariantsKeptUnclustered;
    }

    public void addSubmittedVariantsClustered(long submittedVariantsClustered) {
        this.submittedVariantsClustered += submittedVariantsClustered;
    }

    public long getSubmittedVariantsClustered() {
        return submittedVariantsClustered;
    }

    public void setSubmittedVariantsClustered(long submittedVariantsClustered) {
        this.submittedVariantsClustered = submittedVariantsClustered;
    }

    public void addSubmittedVariantsUpdatedRs(long submittedVariantsUpdatedRs) {
        this.submittedVariantsUpdatedRs += submittedVariantsUpdatedRs;
    }

    public long getSubmittedVariantsUpdatedRs() {
        return submittedVariantsUpdatedRs;
    }

    public void setSubmittedVariantsUpdatedRs(long submittedVariantsUpdatedRs) {
        this.submittedVariantsUpdatedRs = submittedVariantsUpdatedRs;
    }

    public void addSubmittedVariantsUpdateOperationWritten(long submittedVariantsUpdateOperationWritten) {
        this.submittedVariantsUpdateOperationWritten += submittedVariantsUpdateOperationWritten;
    }

    public long getSubmittedVariantsUpdateOperationWritten() {
        return submittedVariantsUpdateOperationWritten;
    }

    public void setSubmittedVariantsUpdateOperationWritten(long submittedVariantsUpdateOperationWritten) {
        this.submittedVariantsUpdateOperationWritten = submittedVariantsUpdateOperationWritten;
    }
}
