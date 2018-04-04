package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertEquals;

public class VariantProcessorTest {

    private VariantProcessor processor;

    @Before
    public void setUp() {
        processor = new VariantProcessor("assembly","taxonomy",
                "project");
    }

    @Test
    public void process() throws Exception {

        Variant variant = new Variant("contig", 1000, 1001, "A", "T");

        SubmittedVariant processed = processor.process((IVariant)variant);

        SubmittedVariant expected = new SubmittedVariant("assembly",
                "taxonomy","project", "contig",
                1000, "A", "T",true);

        assertEquals(expected, processed);
    }
}
