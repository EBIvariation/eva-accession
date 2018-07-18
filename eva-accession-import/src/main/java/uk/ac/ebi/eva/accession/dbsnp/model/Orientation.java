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
package uk.ac.ebi.eva.accession.dbsnp.model;

public enum Orientation {

    FORWARD(1),

    REVERSE(-1),

    UNKNOWN(0);

    private final int value;

    Orientation(int value) {
        this.value = value;
    }

    public static Orientation getOrientation(Integer orientation) {
        if (orientation == 0) {
            return UNKNOWN;
        } else if (orientation == 1) {
            return FORWARD;
        } else if (orientation == -1) {
            return REVERSE;
        } else {
            throw new IllegalArgumentException("Orientation must be +1, -1 or 0");
        }
    }

    public int getValue() {
        return this.value;
    }
}
