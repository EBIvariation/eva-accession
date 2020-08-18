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
package uk.ac.ebi.eva.accession.release.batch.io.multimap;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class MultimapVariantMongoReader extends AccessionedVariantMongoReader {

    private static final Logger logger = LoggerFactory.getLogger(MultimapVariantMongoReader.class);

    // see https://www.ncbi.nlm.nih.gov/books/NBK44455/#Build.your_descriptions_of_mapweight_in
    public static final int NON_SINGLE_LOCATION_MAPPING = 2;

    public MultimapVariantMongoReader(String assemblyAccession, MongoClient mongoClient,
                                      String database, int chunkSize, CollectionNames names) {
        super(assemblyAccession, mongoClient, database, chunkSize, names);
    }

    @Override
    protected List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.and(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession),
                                                  Filters.gte(MAPPING_WEIGHT_FIELD, NON_SINGLE_LOCATION_MAPPING)));
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        Bson lookup = Aggregates.lookup(names.getSubmittedVariantEntity(), ACCESSION_FIELD,
                                        CLUSTERED_VARIANT_ACCESSION_FIELD, SS_INFO_FIELD);
        List<Bson> aggregation = Arrays.asList(match, sort, lookup);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }

    @Override
    protected List<Variant> getVariants(Document clusteredVariant) {
        List<Variant> variants = super.getVariants(clusteredVariant);
        for (Variant variant : variants) {
            variant.getSourceEntries().iterator().next().addAttribute(MAPPING_WEIGHT_KEY,
                                                                      clusteredVariant.get(MAPPING_WEIGHT_FIELD).toString());
        }
        return variants;
    }
}
