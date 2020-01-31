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
package uk.ac.ebi.eva.accession.pipeline.batch.tasklets.reportCheck;

import org.springframework.batch.item.file.LineMapper;

import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.List;

/**
 * Maps a String (in VCF format) to a list of variants.
 * <p>
 * The actual implementation is reused from {@link CoordinatesVcfFactory}.
 */
public class CoordinatesVcfLineMapper implements LineMapper<List<Variant>> {

    private final VariantVcfFactory factory;

    public CoordinatesVcfLineMapper() {
        this.factory = new CoordinatesVcfFactory();
    }

    @Override
    public List<Variant> mapLine(String line, int lineNumber) {
        return factory.create(null, null, line);
    }
}
