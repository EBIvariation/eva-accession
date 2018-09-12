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
package uk.ac.ebi.eva.accession.dbsnp.contig;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ContigMappingTest {

    private static final String SEQNAME_CONTIG = "chrom1";

    private static final String SEQNAME_CONTIG_2 = "2";

    private static final String SEQNAME_CONTIG_UNIQUE_ASSIGNED_MOLECULE = "DCARv2_Chr1";

    private static final String SEQNAME_CONTIG_UNIQUE_ASSIGNED_MOLECULE_2 = "DCARv2_B1";

    private static final String SEQNAME_WITHOUT_SYNONYM = "CHR_without_synonym";

    private static final String SEQNAME_NON_ASSEMBLED = "MMCHR1_RANDOM_CTG1";

    private static final String ASSIGNED_MOLECULE_CONTIG = "1";

    private static final String ASSIGNED_MOLECULE_CONTIG_2 = "unique_assigned_molecule_2";

    private static final String ASSIGNED_MOLECULE_EXAMPLE = "assigned_molecule_example_1";

    private static final String GENBANK_CONTIG = "CM000994.2";

    private static final String GENBANK_CONTIG_UNIQUE_ASSIGNED_MOLECULE = "CM004278.1";

    private static final String GENBANK_WITHOUT_SYNONYM = "GL_without_synonym";

    private static final String REFSEQ_CONTIG = "NC_000067.6";

    private static final String REFSEQ_CONTIG_UNIQUE_ASSIGNED_MOLECULE = "NC_030381.1";

    private static final String REFSEQ_WITHOUT_SYNONYM = "NT_without_synonym";

    private static final String UCSC_CONTIG = "chr1";

    private static final String UCSC_EXAMPLE = "ucsc_example_1";

    private static final String UCSC_WITHOUT_SYNONYM = "UCSC_without_synonym";

    private static final String ASSEMBLY_REPORT_WITH_ASSIGNED_MOLECULE = "/input-files/assembly-report/AssemblyReportUniqueAssignedMolecule.txt";

    private ContigMapping contigMapping;

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
    public void removePrefixOnlyAtTheBeginning() {
        assertEquals("otherprefix_chr45", contigMapping.getContigSynonyms("genbank_example_2").getSequenceName());
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
    public void noSynonymsOfAssignedMoleculeIfThereAreDuplicates() {
        assertNull(contigMapping.getContigSynonyms(ContigMappingTest.ASSIGNED_MOLECULE_EXAMPLE));
    }

    @Test
    public void getSynonymFromAssignedMoleculeIfThereAreDuplicatesOnlyInOtherKeys() {
        assertNotNull(contigMapping.getContigSynonyms(ASSIGNED_MOLECULE_CONTIG_2).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeReturnsNullForNonAssembledMolecules() {
        assertNull(contigMapping.getContigSynonyms(SEQNAME_NON_ASSEMBLED).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeIfThereAreDuplicatesOnlyInOtherKeys() {
        assertEquals(ASSIGNED_MOLECULE_CONTIG_2,
                     contigMapping.getContigSynonyms(SEQNAME_CONTIG_2).getAssignedMolecule());
    }

    @Test
    public void getAssignedMoleculeReturnsNullIfNotAvailable() throws Exception {
        String fileString = ContigMappingTest.class.getResource(ASSEMBLY_REPORT_WITH_ASSIGNED_MOLECULE).toString();
        contigMapping = new ContigMapping(fileString);
        assertNull(contigMapping.getContigSynonyms(SEQNAME_CONTIG_UNIQUE_ASSIGNED_MOLECULE_2).getAssignedMolecule());
    }

    @Test
    public void getUcscReturnsNullIfNotAvailable() throws Exception {
        String fileString = ContigMappingTest.class.getResource(ASSEMBLY_REPORT_WITH_ASSIGNED_MOLECULE).toString();
        contigMapping = new ContigMapping(fileString);
        assertNull(contigMapping.getContigSynonyms(SEQNAME_CONTIG_UNIQUE_ASSIGNED_MOLECULE).getUcsc());
    }

    // get SEQNAME

    @Test
    public void getSeqNameFromSeqName() {
        assertEquals(SEQNAME_CONTIG, contigMapping.getContigSynonyms(SEQNAME_CONTIG).getSequenceName());
    }

    @Test
    public void getSeqNameFromAssignedMolecule() throws Exception {
        String fileString = ContigMappingTest.class.getResource(ASSEMBLY_REPORT_WITH_ASSIGNED_MOLECULE).toString();
        contigMapping = new ContigMapping(fileString);
        assertEquals(SEQNAME_CONTIG_UNIQUE_ASSIGNED_MOLECULE,
                     contigMapping.getContigSynonyms(ASSIGNED_MOLECULE_CONTIG).getSequenceName());
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
