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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantSplittingPolicy.SplitDeterminants;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
public class ClusteredVariantSplittingPolicyTest {

    @Test
    public void testSplittingPolicyWithDifferentNumberOfSupportingVariantsForHashes() {
        ClusteredVariantEntity rs1WithHash1 = createRS(1L, "chr1", 1L, VariantType.SNV);
        ClusteredVariantEntity rs1WithHash2 = createRS(1L, "chr1", 2L, VariantType.SNV);

        // Let's say that hash 1 has three supporting variants (last argument in the triple)
        // and hash 2 has four supporting variants
        SplitDeterminants rs1Hash1 = new SplitDeterminants(rs1WithHash1, rs1WithHash1.getHashedMessage(), 3, 1L);
        SplitDeterminants rs1Hash2 = new SplitDeterminants(rs1WithHash2, rs1WithHash2.getHashedMessage(), 4, 2L);

        // Hash 2 should keep the RS because it has two supporting variants
        SplitDeterminants expectedHashThatKeepsRS1 =
                new SplitDeterminants(rs1WithHash2, rs1WithHash2.getHashedMessage(), 4, 2L);
        SplitDeterminants expectedHashThatShouldBeSplitFromRS1 =
                new SplitDeterminants(rs1WithHash1, rs1WithHash1.getHashedMessage(), 3, 1L);
        ClusteredVariantSplittingPolicy.SplitPriority splitPriority =
                ClusteredVariantSplittingPolicy.prioritise(rs1Hash1, rs1Hash2);
        assertEquals(expectedHashThatKeepsRS1, splitPriority.hashThatShouldRetainOldRS);
        assertEquals(expectedHashThatShouldBeSplitFromRS1, splitPriority.hashThatShouldGetNewRS);
    }

    @Test
    public void testSplittingPolicyWithTieBreaker() {
        ClusteredVariantEntity rs1WithHash1 = createRS(1L, "chr1", 1L, VariantType.SNV);
        ClusteredVariantEntity rs1WithHash2 = createRS(1L, "chr1", 2L, VariantType.SNV);

        // Let's say that hash 1 and hash 2 both have three supporting variants (last argument in the triple)
        // The tie-breaker will be the RS hash that has the oldest SS ID
        SplitDeterminants rs1Hash1 =
                new SplitDeterminants(rs1WithHash1, rs1WithHash1.getHashedMessage(), 3, 1L);
        SplitDeterminants rs1Hash2 =
                new SplitDeterminants(rs1WithHash2, rs1WithHash2.getHashedMessage(), 3, 2L);

        // Hash 1 should keep the RS because it has the oldest SS ID 1
        SplitDeterminants  expectedHashThatKeepsRS1 =
                new SplitDeterminants(rs1WithHash1, rs1WithHash1.getHashedMessage(), 3, 1L);
        SplitDeterminants expectedHashThatShouldBeSplitFromRS1 =
                new SplitDeterminants(rs1WithHash2, rs1WithHash2.getHashedMessage(), 3, 2L);
        ClusteredVariantSplittingPolicy.SplitPriority splitPriority =
                ClusteredVariantSplittingPolicy.prioritise(rs1Hash1, rs1Hash2);
        assertEquals(expectedHashThatKeepsRS1, splitPriority.hashThatShouldRetainOldRS);
        assertEquals(expectedHashThatShouldBeSplitFromRS1, splitPriority.hashThatShouldGetNewRS);
    }

    private ClusteredVariantEntity createRS(Long accession, String contig, Long start, VariantType type) {
        Function<IClusteredVariant, String> hashingFunction =
                new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        ClusteredVariant variant = new ClusteredVariant("asm1", 100, contig, start, type, false, null);
        return new ClusteredVariantEntity(accession, hashingFunction.apply(variant), variant);
    }
}
