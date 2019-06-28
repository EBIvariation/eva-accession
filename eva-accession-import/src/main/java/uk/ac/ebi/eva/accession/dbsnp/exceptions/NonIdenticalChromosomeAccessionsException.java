/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.exceptions;

import static org.springframework.util.StringUtils.hasText;

/**
 * This exception signals a possible issue with the chromosome accession equivalences in a species.
 *
 * Consider this (made up) example of an assembly report:
 *
 * # Sequence-Name	Sequence-Role	Assigned-Molecule	Assigned-Molecule-Location/Type	GenBank-Accn	Relationship	RefSeq-Accn	Assembly-Unit	Sequence-Length	UCSC-style-name
 * Chr01	assembled-molecule	1	Chromosome	CM002812.1	<>	NC_029977.1	Primary Assembly	301019445	na
 *
 * You can see how the GenBank and RefSeq accessions are not identical, but it doesn't assert whether the Sequence Name
 * (usual chromosome names) is identical to the GenBank accessions.
 *
 * We don't expect this to happen, but (at the time of writing)
 * {@link uk.ac.ebi.eva.accession.dbsnp.processors.ContigReplacerProcessor} will replace the chromosome with the
 * GenBank accession if the 'forceImport' is set.
 *
 * Note that this doesn't apply if refseq is not available ("CM123.1 <> na") because then it's clear that the chromosome
 * is identical to the genbank contig, and the replacement "chromosome to genbank" can be safely done.
 */
public class NonIdenticalChromosomeAccessionsException extends RuntimeException {

    private String chromosome;

    private String genBank;

    private String refSeq;

    public NonIdenticalChromosomeAccessionsException(String chromosome, String genBank, String refSeq) {
        this.chromosome = chromosome;
        this.genBank = genBank;
        this.refSeq = refSeq;
    }

    @Override
    public String getMessage() {
        return "It's not completely safe to replace the chromosome name '" + chromosome
               + "' with the INSDC accession '" + genBank
               + "' because the RefSeq '" + refSeq
               + "' and INSDC accessions are not identical. This could mean that dbSNP (RefSeq) "
               + "chromosomes are not the same as EVA (INSDC, GenBank) chromosomes, at least for this "
               + "species.";
    }

    /**
     * This check was extracted to give visibility to this class' documentation
     */
    public static boolean isExceptionApplicable(boolean genbankAndRefseqIdentical, String refSeq) {
        return !genbankAndRefseqIdentical && hasText(refSeq);
    }
}
