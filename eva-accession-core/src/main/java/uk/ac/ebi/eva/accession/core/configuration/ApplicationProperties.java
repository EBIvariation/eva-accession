/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.configuration;

public class ApplicationProperties {

    private VariantAccessioningProperties submitted;

    private VariantAccessioningProperties clustered;

    public VariantAccessioningProperties getSubmitted() {
        return submitted;
    }

    public VariantAccessioningProperties getClustered() {
        return clustered;
    }

    public void setSubmitted(VariantAccessioningProperties submitted) {
        this.submitted = submitted;
    }

    public void setClustered(VariantAccessioningProperties clustered) {
        this.clustered = clustered;
    }

    @Override
    public String toString() {
        return "ApplicationProperties{" +
                "submitted=" + submitted +
                ", clustered=" + clustered +
                '}';
    }
}
