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
package uk.ac.ebi.eva.accession.release.batch.io;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.orderBy;

public class MultimapVariantMongoReader extends AccessionedVariantMongoReader {

    private static final Logger logger = LoggerFactory.getLogger(MultimapVariantMongoReader.class);

    public static final String MAP_WEIGHT_KEY = "mapWeight";

    // see https://www.ncbi.nlm.nih.gov/books/NBK44455/#Build.your_descriptions_of_mapweight_in
    public static final int NON_SINGLE_LOCATION_MAPPING = 2;

    public MultimapVariantMongoReader(String assemblyAccession, MongoClient mongoClient,
                                      String database, int chunkSize) {
        super(assemblyAccession, mongoClient, database, chunkSize);
    }

    @Override
    List<Bson> buildAggregation() {
        Bson match = Aggregates.match(Filters.and(Filters.eq(REFERENCE_ASSEMBLY_FIELD, assemblyAccession),
                                                  Filters.gte(MAP_WEIGHT_KEY, NON_SINGLE_LOCATION_MAPPING)));
        Bson sort = Aggregates.sort(orderBy(ascending(CONTIG_FIELD, START_FIELD)));
        Bson lookup = Aggregates.lookup(DBSNP_SUBMITTED_VARIANT_ENTITY, ACCESSION_FIELD,
                                        CLUSTERED_VARIANT_ACCESSION_FIELD, SS_INFO_FIELD);
        List<Bson> aggregation = Arrays.asList(match, sort, lookup);
        logger.info("Issuing aggregation: {}", aggregation);
        return aggregation;
    }
}
