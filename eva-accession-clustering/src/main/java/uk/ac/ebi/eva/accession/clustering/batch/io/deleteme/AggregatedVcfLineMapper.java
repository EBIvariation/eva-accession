/*
 * Copyright 2016-2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.io.deleteme;

import org.springframework.batch.item.file.LineMapper;
import org.springframework.util.Assert;

import uk.ac.ebi.eva.commons.core.models.Aggregation;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Maps a String (in VCF format, with aggregated samples) to a list of variants.
 * <p>
 * The actual implementation is reused from {@link VariantAggregatedVcfFactory}.
 */
public class AggregatedVcfLineMapper implements LineMapper<List<Variant>> {

    private final String fileId;

    private final String studyId;

    private VariantVcfFactory factory;

    public AggregatedVcfLineMapper(String fileId, String studyId, Aggregation aggregation,
                                   @Nullable String mappingFilePath) throws IOException {
        Assert.notNull(fileId, "file id should not be null");
        Assert.notNull(studyId, "study id should not be null");
        Assert.notNull(aggregation, "aggregation (NONE, BASIC, EXAC, EVS) should not be null");

        this.fileId = fileId;
        this.studyId = studyId;

        Properties mappings = null;
        if (mappingFilePath != null) {
            mappings = FileUtils.getPropertiesFile(new FileInputStream(mappingFilePath));
        }

        switch (aggregation) {
            case EVS:
                factory = new VariantVcfEVSFactory(mappings);
                break;
            case EXAC:
                factory = new VariantVcfExacFactory(mappings);
                break;
            case BASIC:
                factory = new VariantAggregatedVcfFactory(mappings);
                break;
            case NONE:
            default:
                throw new IllegalArgumentException(
                        this.getClass().getSimpleName() + " should be used to read aggregated VCFs only, " +
                                "but the VariantSource.Aggregation is set to " + aggregation);
        }
    }

    @Override
    public List<Variant> mapLine(String line, int lineNumber) throws Exception {
        return factory.create(fileId, studyId, line);
    }

    public void setIncludeIds(boolean includeIds) {
        factory.setIncludeIds(includeIds);
    }

    public boolean isIncludeIds() {
        return factory.isIncludeIds();
    }

    public void setRequireEvidence(boolean includeIds) {
        factory.setRequireEvidence(includeIds);
    }

    public boolean isRequireEvidence() {
        return factory.isRequireEvidence();
    }
}
