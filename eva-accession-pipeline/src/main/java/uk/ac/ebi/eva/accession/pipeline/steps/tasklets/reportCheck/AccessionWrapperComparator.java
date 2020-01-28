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

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.IVariant;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AccessionWrapperComparator implements Comparator<AccessionWrapper<ISubmittedVariant, String, Long>> {

    private Map<String, Integer> contigOrder;

    /**
     * @param variants      The contig order will be extracted from this initial variant list
     */
    public AccessionWrapperComparator(List<? extends IVariant> variants) {
        this.contigOrder = getContigOrder(variants.stream().map(IVariant::getChromosome).collect(Collectors.toList()));
    }

    private Map<String, Integer> getContigOrder(Iterable<String> contigs) {
        Map<String, Integer> contigsOrder = new HashMap<>();
        int nextIndex = 0;
        for (String contig : contigs) {
            if (!contigsOrder.containsKey(contig)) {
                contigsOrder.put(contig, nextIndex++);
            }
        }
        return contigsOrder;
    }

    @Override
    public int compare(AccessionWrapper<ISubmittedVariant, String, Long> firstAccession,
                       AccessionWrapper<ISubmittedVariant, String, Long> secondAccession) {
        Integer firstAccessionOrder = contigOrder.get(firstAccession.getData().getContig());
        Integer secondAccessionOrder = contigOrder.get(secondAccession.getData().getContig());
        if (firstAccessionOrder == null || secondAccessionOrder == null) {
            String missingContigInOrdering = firstAccessionOrder == null ? firstAccession.getData().getContig()
                                                                         : secondAccession.getData().getContig();
            throw new IllegalStateException("AccessionWrapperComparator can not compare "
                                                    + firstAccession.getData().getContig()
                                                    + " and "
                                                    + secondAccession.getData().getContig()
                                                    + ", because "
                                                    + missingContigInOrdering
                                                    + " was not contained in the initial list of variants that is "
                                                    + "used to extract the order: " + contigOrder.toString());
        }
        Integer contigComparison = firstAccessionOrder - secondAccessionOrder;
        if (contigComparison < 0) {
            return -1;
        } else if (contigComparison > 0) {
            return 1;
        } else {
            long positionComparison = firstAccession.getData().getStart() - secondAccession.getData().getStart();
            return Long.signum(positionComparison);
        }
    }
}
