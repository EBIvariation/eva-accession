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
package uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck;

import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

import java.util.Comparator;

public class AccessionWrapperComparator implements Comparator<AccessionWrapper<ISubmittedVariant, String, Long>> {

    @Override
    public int compare(AccessionWrapper<ISubmittedVariant, String, Long> firstAccession,
                       AccessionWrapper<ISubmittedVariant, String, Long> secondAccession) {
        int contigComparation = new AlphanumComparator().compare(firstAccession.getData().getContig(),
                                                                 secondAccession.getData().getContig());
        if (contigComparation < 0) {
            return -1;
        } else if (contigComparation > 0) {
            return 1;
        } else {
            long positionComparation = firstAccession.getData().getStart() - secondAccession.getData().getStart();
            if (positionComparation < 0) {
                return -1;
            } else if (positionComparation > 0) {
                return 1;
            } else {
                return 0;
            }
        }
    }
}
