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
package uk.ac.ebi.eva.accession.clustering.test;

import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class VariantAssertions {

    public static void assertAccessionEqual(Set<Long> expected,
                                            List<? extends ClusteredVariantEntity> clusteredVariants) {
        assertEquals(expected,
                     clusteredVariants.stream().map(v -> v.getAccession()).collect(Collectors.toSet()));
    }

    public static void assertAssemblyAccessionEqual(TreeSet<String> expected,
                                                    List<? extends ClusteredVariantEntity> clusteredVariants) {
        assertEquals(expected,
                     clusteredVariants.stream().map(v -> v.getAssemblyAccession()).collect(Collectors.toSet()));
    }

    public static void assertReferenceSequenceAccessionEqual(TreeSet<String> expected,
                                                             List<? extends SubmittedVariantEntity> submittedVariants) {
        assertEquals(expected,
                     submittedVariants.stream()
                                      .map(v -> v.getReferenceSequenceAccession())
                                      .collect(Collectors.toSet()));
    }

    public static void assertClusteredVariantAccessionEqual(TreeSet<Long> expected,
                                                            List<? extends SubmittedVariantEntity> submittedVariants) {
        assertEquals(expected,
                     submittedVariants.stream().map(v -> v.getClusteredVariantAccession()).collect(Collectors.toSet()));
    }
}
