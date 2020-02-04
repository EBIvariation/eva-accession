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
package uk.ac.ebi.eva.accession.dbsnp.batch.processors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import static org.junit.Assert.assertEquals;

public class ContigReplacerProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 1111;

    private static final long START = 5;

    private static final String REFERENCE_ALLELE = "C";

    private static final String ALTERNATE_ALLELE = "T";

    private static final String ASSIGNED_MOLECULE = "1";


    private static final String MISSING_CHROMOSOME = "23";

    private static final String SEQNAME_1 = "22";

    private static final String SEQNAME_ASSEMBLED = "chrom1";

    private static final String GENBANK_CONTIG = "CM000994.2";

    private static final String GENBANK_1 = "genbank_example_1";

    private static final String REFSEQ_1 = "refseq_example_1";

    private static final String MISSING_CONTIG = "NT_missing";

    private static final String UCSC_1 = "ucsc_example_1";

    private static final Long SS_ID = 12345L;

    private static final Long RS_ID = 56789L;

    private static final String GENBANK_ASSEMBLY_ACCESSION = "GCA_000001635.8";

    private static final String REFSEQ_NON_IDENTICAL = "NT_without_synonym";

    private static final String REFSEQ_NON_IDENTICAL_2 = "NT_without_synonym_2";

    private static final String GENBANK_NON_IDENTICAL = "GL_without_synonym";

    private static final String SEQNAME_NON_IDENTICAL = "CHR_without_synonym";

    private static final String ASSIGNED_MOLECULE_ASSEMBLED_NON_IDENTICAL = "6";

    private static final String GENBANK_ASSEMBLED_NON_IDENTICAL = "CM000999.2";

    private static final String SEQNAME_ASSEMBLED_NON_IDENTICAL = "chrom6";

    private static final String SEQNAME_NON_ASSEMBLED = "NON_ASSEMBLED_CHR_42";

    private static final String ASSIGNED_MOLECULE_NON_ASSEMBLED = "42";

    private static final String GENBANK_NON_ASSEMBLED = "GL_NON_ASSEMBLED";

    private static final String REFSEQ_NON_ASSEMBLED = "NT_NON_ASSEMBLED";

    private static final String SEQNAME_NON_ASSEMBLED_NON_IDENTICAL = "CHR_without_synonym_2";

    private static final String ASSIGNED_MOLECULE_NON_ASSEMBLED_NON_IDENTICAL = "100";

    private static final String GENBANK_NON_ASSEMBLED_NON_IDENTICAL = "GL_without_synonym_2";

    private static final String REFSEQ_NON_ASSEMBLED_NON_IDENTICAL = "NT_without_synonym_2";

    private ContigReplacerProcessor processor;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        String fileString = ContigReplacerProcessorTest.class.getResource(
                "/input-files/assembly-report/GCA_000001635.8_Mus_musculus-grcm38.p6_assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        processor = new ContigReplacerProcessor(contigMapping, GENBANK_ASSEMBLY_ACCESSION);
    }

    // test special cases of combinations of "missing in assembly report"

    @Test
    public void shouldThrowIfContigAndChromosomeAreMissingInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, MISSING_CONTIG);
        thrown.expect(IllegalStateException.class);
        processor.process(input);
    }

    private SubSnpNoHgvs newMockSubSnpNoHgvs(String chromosome, String contig) {
        return new SubSnpNoHgvs(SS_ID, RS_ID, REFERENCE_ALLELE, ALTERNATE_ALLELE, ASSEMBLY, "", "",
                                chromosome, START, contig, START, DbsnpVariantType.SNV,
                                Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD, true, true,
                                true, true, null, null, TAXONOMY);
    }

    @Test
    public void shouldNotThrowIfContigAndChromosomeAreFoundInDifferentLinesInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_ASSEMBLED, GENBANK_1);
        processor.process(input);
    }

    @Test
    public void convertChromosomeIfContigIsMissingInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_1, MISSING_CONTIG);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
    }

    @Test
    public void convertContigIfChromosomeIsMissingInAssemblyReport() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, REFSEQ_1);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
    }

    // test special cases of non identical contigs

    @Test
    public void convertChromosomeToGenbankIfAssembledMoleculeEvenIfGenbankAndRefseqAreNotIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_ASSEMBLED_NON_IDENTICAL, MISSING_CONTIG);
        assertEquals(GENBANK_ASSEMBLED_NON_IDENTICAL, processor.process(input).getContigName());
    }

    @Test
    public void convertAssignedMoleculeToGenbankIfAssembledMoleculeEvenIfGenbankAndRefseqAreNotIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(ASSIGNED_MOLECULE_ASSEMBLED_NON_IDENTICAL, MISSING_CONTIG);
        assertEquals(GENBANK_ASSEMBLED_NON_IDENTICAL, processor.process(input).getContigName());
    }

    @Test
    public void doNotConvertRefseqToGenbankIfGenbankAndRefseqAreNotIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, REFSEQ_NON_IDENTICAL);
        assertEquals(REFSEQ_NON_IDENTICAL, processor.process(input).getContigName());
    }

    // test special cases of non-assembled assigned molecules

    @Test
    public void convertChromosomeIfNonAssembled() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_NON_ASSEMBLED, MISSING_CONTIG);
        assertEquals(GENBANK_NON_ASSEMBLED, processor.process(input).getContigName());
    }

    @Test
    public void doNotConvertAssignedMoleculeIfNonAssembled() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(ASSIGNED_MOLECULE_NON_ASSEMBLED, REFSEQ_NON_IDENTICAL);
        assertEquals(REFSEQ_NON_IDENTICAL, processor.process(input).getContigName());
    }

    @Test
    public void convertContigIfNonAssembled() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, REFSEQ_NON_ASSEMBLED);
        assertEquals(GENBANK_NON_ASSEMBLED, processor.process(input).getContigName());
    }

    // test special cases of non-assembled non-identical

    @Test
    public void convertChromosomeIfNonAssembledMoleculeAndNonIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_NON_ASSEMBLED_NON_IDENTICAL, MISSING_CONTIG);
        assertEquals(GENBANK_NON_ASSEMBLED_NON_IDENTICAL, processor.process(input).getContigName());
    }

    @Test
    public void doNotConvertAssignedMoleculeIfNonAssembledMoleculeAndNonIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(ASSIGNED_MOLECULE_NON_ASSEMBLED_NON_IDENTICAL, REFSEQ_NON_IDENTICAL);
        assertEquals(REFSEQ_NON_IDENTICAL, processor.process(input).getContigName());
    }

    @Test
    public void doNotConvertContigIfNonAssembledMoleculeAndNonIdentical() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(MISSING_CHROMOSOME, REFSEQ_NON_IDENTICAL_2);
        assertEquals(REFSEQ_NON_IDENTICAL_2, processor.process(input).getContigName());
    }

    // test regular cases

    @Test
    public void convertVariantFromSeqNameToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_1, SEQNAME_1);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromAssignedMoleculeToGenBankIfAssembledMolecule() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(ASSIGNED_MOLECULE, ASSIGNED_MOLECULE);
        assertEquals(GENBANK_CONTIG, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromGenBankToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(GENBANK_1, GENBANK_1);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromRefSeqToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(REFSEQ_1, REFSEQ_1);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
    }

    @Test
    public void convertVariantFromUcscToGenBank() throws Exception {
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(UCSC_1, UCSC_1);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
    }

    // tests about priority between contig and chromosome

    @Test
    public void prioritiseChromosomeCoordinatesOverRefseq() throws Exception {
        long chromosomeStart = 100;
        long contigStart = 200;
        SubSnpNoHgvs input = newMockSubSnpNoHgvs(SEQNAME_1, chromosomeStart, REFSEQ_1, contigStart);
        assertEquals(GENBANK_1, processor.process(input).getContigName());
        assertEquals(chromosomeStart, processor.process(input).getContigStart());
    }

    private SubSnpNoHgvs newMockSubSnpNoHgvs(String chromosome, long chromosomeStart, String contig, long contigStart) {
        return new SubSnpNoHgvs(SS_ID, RS_ID, REFERENCE_ALLELE, ALTERNATE_ALLELE, ASSEMBLY, "", "",
                                chromosome, chromosomeStart, contig, contigStart, DbsnpVariantType.SNV,
                                Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD, true, true,
                                true, true, null, null, TAXONOMY);
    }
}
