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
package uk.ac.ebi.eva.remapping.ingest.batch.listeners;

public class RemappingIngestCounts {

    private long remappedVariantsIngested;

    private long remappedVariantsSkipped;

    private long remappedVariantsDiscarded;

    public RemappingIngestCounts() {
        this.remappedVariantsIngested = 0;
        this.remappedVariantsSkipped = 0;
        this.remappedVariantsDiscarded = 0;
    }

    public long getRemappedVariantsIngested() {
        return remappedVariantsIngested;
    }

    public void addRemappedVariantsIngested(long remappedVariantsIngested) {
        this.remappedVariantsIngested += remappedVariantsIngested;
    }

    public long getRemappedVariantsSkipped() {
        return remappedVariantsSkipped;
    }

    public void addRemappedVariantsSkipped(long remappedVariantsSkipped) {
        this.remappedVariantsSkipped += remappedVariantsSkipped;
    }

    public long getRemappedVariantsDiscarded() {
        return remappedVariantsDiscarded;
    }

    public void addRemappedVariantsDiscarded(long remappedVariantsDiscarded) {
        this.remappedVariantsDiscarded += remappedVariantsDiscarded;
    }
}
