package uk.ac.ebi.eva.accession.core.io;

import org.springframework.batch.item.file.LineMapper;
import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.List;

public class AccessionedVcfLineMapper implements LineMapper<List<Variant>> {
    private final VariantVcfFactory factory = new AccessionedVcfFactory();

    public AccessionedVcfLineMapper() {
    }

    public List<Variant> mapLine(String line, int lineNumber) {
        return this.factory.create((String) null, (String) null, line);
    }
}
