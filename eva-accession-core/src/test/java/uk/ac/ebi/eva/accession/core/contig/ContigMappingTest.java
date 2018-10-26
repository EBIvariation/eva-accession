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
package uk.ac.ebi.eva.accession.core.contig;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ContigMappingTest {

    private static final String SEQNAME_CONTIG = "chrom1";

    private static final String SEQNAME_CONTIG_UNAVAILABLE_ASSIGNED_MOLECULE = "MMCHR5_RANDOM_CTG5";

    private static final String SEQNAME_CONTIG_UNAVAILABLE_UCSC = "MMCHR5_RANDOM_CTG3";

    private static final String SEQNAME_WITHOUT_SYNONYM = "CHR_without_synonym";

    private static final String SEQNAME_NON_ASSEMBLED = "MMCHR1_RANDOM_CTG1";

    private static final String ASSIGNED_MOLECULE_CONTIG = "1";

    private static final String GENBANK_CONTIG = "CM000994.2";

    private static final String GENBANK_WITHOUT_SYNONYM = "GL_without_synonym";

    private static final String REFSEQ_CONTIG = "NC_000067.6";

    private static final String REFSEQ_WITHOUT_SYNONYM = "NT_without_synonym";

    private static final String UCSC_CONTIG = "chr1";

    private static final String UCSC_WITHOUT_SYNONYM = "UCSC_without_synonym";

    private static final String ASSEMBLED_MOLECULE_ROLE = "assembled-molecule";

    private ContigMapping contigMapping;

    private static final int TOTAL_ROWS = 24;

    private static final int NON_ASSEMBLED_MOLECULE_ROWS = 11;

    private static final int MISSING_UCSC_ROWS = 1;

    @Before
    public void setUp() throws Exception {
        String fileString = ContigMappingTest.class.getResource("/input-files/assembly-report/AssemblyReport.txt").toString();
        contigMapping = new ContigMapping(fileString);
    }

    // tests for special cases

    @Test
    public void matchWhenVcfHasPrefixes() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getGenBank());
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getSequenceName());
    }

    @Test
    public void noSynonyms() {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(REFSEQ_WITHOUT_SYNONYM);
        assertNotNull(contigSynonyms);
        assertFalse(contigSynonyms.isIdenticalGenBankAndRefSeq());
        assertEquals(SEQNAME_WITHOUT_SYNONYM, contigSynonyms.getSequenceName());
        assertNull(contigSynonyms.getAssignedMolecule());
        assertEquals(GENBANK_WITHOUT_SYNONYM, contigSynonyms.getGenBank());
        assertEquals(REFSEQ_WITHOUT_SYNONYM, contigSynonyms.getRefSeq());
        assertEquals(UCSC_WITHOUT_SYNONYM, contigSynonyms.getUcsc());
    }

    @Test
    public void getSynonymsOfDuplicatedAssignedMolecule() {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(ASSIGNED_MOLECULE_CONTIG);
        assertNotNull(contigSynonyms);
        assertEquals(ASSEMBLED_MOLECULE_ROLE, contigSynonyms.getSequenceRole());
        assertNotNull(contigSynonyms.getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeReturnsNullForNonAssembledMolecules() {
        assertNull(contigMapping.getContigSynonyms(SEQNAME_NON_ASSEMBLED).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeReturnsNullIfNotAvailable() throws Exception {
        assertNull(contigMapping.getContigSynonyms(SEQNAME_CONTIG_UNAVAILABLE_ASSIGNED_MOLECULE).getAssignedMolecule());
    }

    @Test
    public void getUcscReturnsNullIfNotAvailable() throws Exception {
        assertNull(contigMapping.getContigSynonyms(SEQNAME_CONTIG_UNAVAILABLE_UCSC).getUcsc());
    }

    @Test
    public void checkAllEntriesWereLoaded() {
        assertEquals(TOTAL_ROWS, contigMapping.sequenceNameToSynonyms.size());
        assertEquals(TOTAL_ROWS - NON_ASSEMBLED_MOLECULE_ROWS, contigMapping.assignedMoleculeToSynonyms.size());
        assertEquals(TOTAL_ROWS, contigMapping.genBankToSynonyms.size());
        assertEquals(TOTAL_ROWS, contigMapping.refSeqToSynonyms.size());
        assertEquals(TOTAL_ROWS - MISSING_UCSC_ROWS, contigMapping.ucscToSynonyms.size());
    }

    // get SEQNAME

    @Test
    public void getSeqNameFromSeqName() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromAssignedMolecule() throws Exception {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(ASSIGNED_MOLECULE_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromGenBank() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromRefSeq() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromUcsc() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getSequenceName());
    }

    // get ASSIGNED_MOLECULE

    @Test
    public void getAssignedMoleculeFromSeqName() throws Exception {
        assertEquals(ASSIGNED_MOLECULE_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeFromAssignedMolecule() throws Exception {
        assertEquals(ASSIGNED_MOLECULE_CONTIG,
                     contigMapping.getContigSynonyms(ASSIGNED_MOLECULE_CONTIG).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeFromGenBank() throws Exception {
        assertEquals(ASSIGNED_MOLECULE_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeFromRefSeq() throws Exception {
        assertEquals(ASSIGNED_MOLECULE_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeFromUcsc() throws Exception {
        assertEquals(ASSIGNED_MOLECULE_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getAssignedMolecule());
    }

    // get GENBANK

    @Test
    public void getGenBankFromSeqName() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getGenBank());
    }

    @Test
    public void getGenBankFromGenBank() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getGenBank());
    }

    @Test
    public void getGenBankFromRefSeq() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getGenBank());
    }

    @Test
    public void getGenBankFromUcsc() {
        assertEquals(GENBANK_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getGenBank());
    }

    // get REFSEQ

    @Test
    public void getRefSeqFromSeqName() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getRefSeq());
    }

    @Test
    public void getRefSeqFromGenBank() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getRefSeq());
    }

    @Test
    public void getRefSeqFromRefSeq() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getRefSeq());
    }

    @Test
    public void getRefSeqFromUcsc() {
        assertEquals(REFSEQ_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getRefSeq());
    }

    // get UCSC

    @Test
    public void getUcscFromSeqName() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getUcsc());
    }

    @Test
    public void getUcscFromGenBank() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(GENBANK_CONTIG).getUcsc());
    }

    @Test
    public void getUcscFromRefSeq() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(REFSEQ_CONTIG).getUcsc());
    }

    @Test
    public void getUcscFromUcsc() {
        assertEquals(UCSC_CONTIG, contigMapping.getContigSynonyms(UCSC_CONTIG).getUcsc());
    }

}
