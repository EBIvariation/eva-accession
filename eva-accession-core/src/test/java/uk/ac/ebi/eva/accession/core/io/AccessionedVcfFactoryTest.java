package uk.ac.ebi.eva.accession.core.io;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.ebi.eva.commons.core.models.factories.CoordinatesVcfFactory;
import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;


public class AccessionedVcfFactoryTest {

    private static final String FILE_ID = "fileId";

    private static final String STUDY_ID = "studyId";

    private static VariantVcfFactory factory;

    @BeforeClass
    public static void setupClass() {
        factory = new AccessionedVcfFactory();
    }

    @Test
    public void testDontRemoveChrPrefixInAnyCase() {
        String line;

        line = "chr1\t1000\t.\tT\tG\t.\t.\t.";
        List<Variant> expResult = Collections.singletonList(new Variant("chr1", 1000, 1000, "T", "G"));
        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "Chr1\t1000\t.\tT\tG\t.\t.\t.";
        expResult = Collections.singletonList(new Variant("Chr1", 1000, 1000, "T", "G"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "CHR1\t1000\t.\tT\tG\t.\t.\t.";
        expResult = Collections.singletonList(new Variant("CHR1", 1000, 1000, "T", "G"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);
    }

    @Test
    public void testCreateVariantFromVcfSameLengthRefAlt() {
        // Test when there are differences at the end of the sequence
        String line = "1\t1000\trs123\tTCACCC\tTGACGG\t.\t.\t.";

        List<Variant> expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1005, "CACCC", "GACGG"));

        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        // Test when there are not differences at the end of the sequence
        line = "1\t1000\trs123\tTCACCC\tTGACGC\t.\t.\t.";

        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1004, "CACC", "GACG"));

        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);
    }

    @Test
    public void testCreateVariantFromVcfInsertionEmptyRef() {
        String line = "1\t1000\trs123\t.\tTGACGC\t.\t.\t.";

        List<Variant> expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1000 + "TGACGC".length() - 1, "", "TGACGC"));

        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);
    }

    @Test
    public void testCreateVariantFromVcfDeletionEmptyAlt() {
        String line = "1\t999\trs123\tGTCACCC\tG\t.\t.\t.";

        List<Variant> expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1000 + "TCACCC".length() - 1, "TCACCC", ""));

        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);
    }

    @Test
    public void testCreateVariantFromVcfIndelNotEmptyFields() {
        String line = "1\t1000\trs123\tCGATT\tTAC\t.\t.\t.";

        List<Variant> expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1000 + "CGATT".length() - 1, "CGATT", "TAC"));
        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tAT\tA\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1001, "T", ""));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tGATC\tG\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1003, "ATC", ""));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\t.\tATC\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1002, "", "ATC"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tA\tATC\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1002, "", "TC"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tAC\tACT\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1002, 1002, "", "T"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        // Printing those that are not currently managed
        line = "1\t1000\trs123\tAT\tT\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1000, "A", ""));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tATC\tTC\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1000, "A", ""));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tATC\tAC\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1001, "T", ""));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tAC\tATC\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1001, 1001, "", "T"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);

        line = "1\t1000\trs123\tATC\tGC\t.\t.\t.";
        expResult = new LinkedList<>();
        expResult.add(new Variant("1", 1000, 1001, "AT", "G"));
        result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);
    }

    @Test
    public void testCreateVariantFromVcfCoLocatedVariants_MainFields() {
        String line = "1\t10040\trs123\tTGACGTAACGATT\tT,TGACGTAACGGTT,TGACGTAATAC\t.\t.\t.\tGT\t0/0\t0/1\t0/2\t1/3"; // 4 samples

        // Check proper conversion of main fields
        List<Variant> expResult = new LinkedList<>();
        expResult.add(new Variant("1", 10041, 10040 + "TGACGTAACGAT".length(), "GACGTAACGATT", ""));
        expResult.add(new Variant("1", 10050, 10050 + "A".length() - 1, "A", "G"));
        expResult.add(new Variant("1", 10048, 10048 + "CGATT".length() - 1, "CGATT", "TAC"));

        List<Variant> result = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expResult, result);
    }

    /**
     * Unlike the other factories, the Coordinates one does store the id by default.
     */
    @Test
    public void testVariantIdsByDefault() {
        // test that an ID is NOT ignored
        checkIds(factory, "1\t1000\trs123\tC\tT\t.\t.\t.", Collections.singleton("rs123"));

        // test that several ID are ignored
        checkIds(factory, "1\t1000\trs123;rs456\tC\tT\t.\t.\t.", new HashSet<>(Arrays.asList("rs123", "rs456")));

        // test that a missing ID ('.') is not added to the IDs set
        checkIds(factory, "1\t1000\t.\tC\tT\t.\t.\t.", Collections.emptySet());
    }

    @Test
    public void testVariantIdsEnabled() {
        // EVA-1898 - needed for eva-accession-clustering, test that ID is read if explicitly configured
        VariantVcfFactory accessionedVariantFactory = new CoordinatesVcfFactory();
        accessionedVariantFactory.setIncludeIds(true);

        // test that an ID is properly read
        checkIds(accessionedVariantFactory, "1\t1000\trs123\tC\tT\t.\t.\t.", Collections.singleton("rs123"));

        // test that a missing ID ('.') is not added to the IDs set
        checkIds(factory, "1\t1000\t.\tC\tT\t.\t.\t.", Collections.emptySet());
        checkIds(accessionedVariantFactory, "1\t1000\trs123;.\tC\tT\t.\t.\t.", Collections.singleton("rs123"));

        // test that the ';' is used as the ID separator (as of VCF 4.2)
        checkIds(accessionedVariantFactory, "1\t1000\trs123;rs456\tC\tT\t.\t.\t.",
                Stream.of("rs123", "rs456").collect(Collectors.toSet()));

        // test that the ',' is NOT used as the ID separator (as of VCF 4.2)
        checkIds(accessionedVariantFactory, "1\t1000\trs123,rs456\tC\tT\t.\t.\t.",
                Collections.singleton("rs123,rs456"));
    }

    @Test
    public void testVariantIdsDisabled() {
        // ignore ids if explicitly configured, to comply with the interface
        VariantVcfFactory nonAccessionedVariantFactory = new CoordinatesVcfFactory();
        nonAccessionedVariantFactory.setIncludeIds(false);
        Set<String> emptySet = Collections.emptySet();

        // test that an ID is ignored
        checkIds(nonAccessionedVariantFactory, "1\t1000\trs123\tC\tT\t.\t.\t.", emptySet);

        // test that several ID are ignored
        checkIds(nonAccessionedVariantFactory, "1\t1000\trs123;rs456\tC\tT\t.\t.\t.", emptySet);

        // test that a missing ID ('.') is not added to the IDs set
        checkIds(nonAccessionedVariantFactory, "1\t1000\t.\tC\tT\t.\t.\t.", emptySet);
    }

    private void checkIds(VariantVcfFactory variantVcfFactory, String vcfLine, Set<String> expectedIds) {
        List<Variant> expectedVariants = new LinkedList<>();
        expectedVariants.add(new Variant("1", 1000, 1000, "C", "T"));
        expectedVariants.get(0).setIds(expectedIds);

        List<Variant> parsedVariants = variantVcfFactory.create(FILE_ID, STUDY_ID, vcfLine);

        assertEquals(expectedVariants, parsedVariants);
        assertEquals(expectedIds, parsedVariants.get(0).getIds());
    }

    @Test
    public void testReadContextBaseVariant() {
        String line = "chr1\t1000\t.\tGCAG\tG\t.\t.\t.";
        List<Variant> expectedResult = Collections.singletonList(new Variant("chr1", 1001, 1003, "CAG", ""));
        List<Variant> actualResult = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expectedResult, actualResult);

        line = "chr1\t1000\t.\tG\tGG\t.\t.\t.";
        expectedResult = Collections.singletonList(new Variant("chr1", 1001, 1001, "", "G"));
        actualResult = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expectedResult, actualResult);

        line = "chr1\t661015\t.\tCC\tCCCACC\t.\t.\t.";
        expectedResult = Collections.singletonList(new Variant("chr1", 661015, 661018, "", "CCCA"));
        actualResult = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expectedResult, actualResult);

        line = "chr1\t661014\t.\tA\tACCCA\t.\t.\t.";
        expectedResult = Collections.singletonList(new Variant("chr1", 661015, 661018, "", "CCCA"));
        actualResult = factory.create(FILE_ID, STUDY_ID, line);
        assertEquals(expectedResult, actualResult);
    }
}