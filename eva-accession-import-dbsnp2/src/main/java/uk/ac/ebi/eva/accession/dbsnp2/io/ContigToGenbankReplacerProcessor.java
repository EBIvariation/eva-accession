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
package uk.ac.ebi.eva.accession.dbsnp2.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

import java.util.HashSet;
import java.util.Set;

/**
 * Converts the contig to its GenBank synonym when possible. If the synonym can't be determined it keeps the contig as
 * is
 */
public class ContigToGenbankReplacerProcessor
    implements ItemProcessor<DbsnpClusteredVariantEntity, DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ContigToGenbankReplacerProcessor.class);

    private ContigMapping contigMapping;

    private Set<String> processedContigs;

    public ContigToGenbankReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
        this.processedContigs = new HashSet<>();
    }

    @Override
    public DbsnpClusteredVariantEntity process(DbsnpClusteredVariantEntity variant) {
        String contigName = variant.getContig();
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contigName);

        StringBuilder message = new StringBuilder();
        if (contigMapping.isGenbankReplacementPossible(contigName, contigSynonyms, message)) {
            ClusteredVariant newVariant = new ClusteredVariant(variant.getAssemblyAccession(),
                                                               variant.getTaxonomyAccession(),
                                                               contigSynonyms.getGenBank(),
                                                               variant.getStart(),
                                                               variant.getType(),
                                                               variant.isValidated(),
                                                               variant.getCreatedDate());
            return new DbsnpClusteredVariantEntity(variant.getAccession(),
                                                   variant.getHashedMessage(),
                                                   newVariant,
                                                   variant.getVersion());
        } else {
            if (!processedContigs.contains(contigName)) {
                logger.warn(message.toString());
                processedContigs.add(contigName);
            }
            return variant;
        }
    }
}
