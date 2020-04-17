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
package uk.ac.ebi.eva.accession.core.batch.io;

import uk.ac.ebi.eva.commons.core.models.VariantCoreFields;
import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Transforms a String (in VCF format) to a list of variants, keeping only the basic fields (coordinates):
 * chromosome, position, id, reference, alternate.
 */
public class CoordinatesVcfFactory extends VariantVcfFactory {

    @Override
    public List<Variant> create(String fileId, String studyId, String line)
            throws IllegalArgumentException, NonVariantException, IncompleteInformationException {
        String[] fields = line.split("\t", 6);
        String chromosome = getChromosomeWithoutPrefix(fields);
        long position = getPosition(fields);
        Set<String> ids = getIds(fields);
        String reference = getReference(fields);
        String[] alternateAlleles = getAlternateAlleles(fields);

        List<Variant> variants = new LinkedList<>();
        for (String alternateAllele : alternateAlleles) {
            VariantCoreFields keyFields;
            try {
                keyFields = new VariantCoreFields(chromosome, position, reference, alternateAllele);
            } catch (NonVariantException e) {
                continue;
            }
            Variant variant = new Variant(chromosome, keyFields.getStart(), keyFields.getEnd(),
                                          keyFields.getReference(), keyFields.getAlternate());
            variant.setIds(ids);
            if (ids.size() > 0) {
                variant.setMainId(ids.iterator().next());
            }
            variants.add(variant);
        }
        return variants;
    }

    @Override
    protected Set<String> getIds(String[] fields) {
        String[] idsSplit = fields[2].split(";");
        Set<String> ids;
        if (idsSplit.length == 1 && ".".equals(idsSplit[0])) {
            ids = Collections.emptySet();
        } else {
            ids = new HashSet<>(Arrays.asList(idsSplit));
        }
        return ids;
    }

    @Override
    protected void parseSplitSampleData(VariantSourceEntry variantSourceEntry, String[] strings, int i) {
        throw new UnsupportedOperationException("This factory doesn't support sample parsing");
    }

}
