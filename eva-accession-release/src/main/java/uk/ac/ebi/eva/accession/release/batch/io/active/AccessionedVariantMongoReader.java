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

package uk.ac.ebi.eva.accession.release.batch.io.active;

import com.mongodb.MongoClient;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Meta;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.EVAObjectModelUtils;
import uk.ac.ebi.eva.accession.core.batch.io.MongoDbCursorItemReader;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader;
import uk.ac.ebi.eva.accession.release.collectionNames.CollectionNames;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.VariantTypeToSOAccessionMap;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;


public class AccessionedVariantMongoReader extends VariantMongoAggregationReader
        implements ItemStreamReader<List<Variant>> {

    private MongoDbCursorItemReader<ClusteredVariantEntity> cursorItemReader;

    private MongoTemplate mongoTemplate;

    public AccessionedVariantMongoReader(String assemblyAccession, int taxonomyAccession,
                                         MongoClient mongoClient, MongoTemplate mongoTemplate, String database,
                                         int chunkSize, CollectionNames names) {
        super(assemblyAccession, taxonomyAccession, mongoClient, database, chunkSize, names);
        this.mongoTemplate = mongoTemplate;
        this.cursorItemReader = new MongoDbCursorItemReader<>();
        Class<? extends ClusteredVariantEntity> className =
                names.getClusteredVariantEntity().equals("clusteredVariantEntity")?
                        ClusteredVariantEntity.class: DbsnpClusteredVariantEntity.class;
        this.cursorItemReader.setTargetType(className);
        this.cursorItemReader.setTemplate(mongoTemplate);
        Query queryToGetClusteredVariants = query(where(REFERENCE_ASSEMBLY_FIELD)
                                                          .is(this.assemblyAccession)
                                                          .and(MAPPING_WEIGHT_FIELD).exists(false));
        Meta meta = new Meta();
        meta.addFlag(Meta.CursorOption.NO_TIMEOUT);
        queryToGetClusteredVariants.setMeta(meta);
        this.cursorItemReader.setQuery(queryToGetClusteredVariants);
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.cursorItemReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        this.cursorItemReader.close();
    }

    protected List<Variant> getVariants(ClusteredVariantEntity clusteredVariant,
                                        List<SubmittedVariantEntity> submittedVariants) {
        String contig = clusteredVariant.getContig();
        long start = clusteredVariant.getStart();
        long rs = clusteredVariant.getAccession();
        String type = clusteredVariant.getType().toString();
        String sequenceOntology = VariantTypeToSOAccessionMap.getSequenceOntologyAccession(VariantType.valueOf(type));
        boolean validated = clusteredVariant.getModel().isValidated();
        boolean remappedRS = submittedVariants.stream().allMatch(sve -> Objects.nonNull(sve.getRemappedFrom()));

        Map<String, Variant> variants = new HashMap<>();
        for (SubmittedVariantEntity submittedVariant : submittedVariants) {
            long submittedVariantStart = submittedVariant.getStart();
            String submittedVariantContig = submittedVariant.getContig();
            if (!isSameLocation(contig, start, submittedVariantContig, submittedVariantStart, type)) {
                continue;
            }
            String reference = submittedVariant.getReferenceAllele();
            String alternate = submittedVariant.getAlternateAllele();
            String study = submittedVariant.getProjectAccession();
            boolean submittedVariantValidated = submittedVariant.getModel().isValidated();
            boolean allelesMatch = submittedVariant.getModel().isAllelesMatch();
            boolean assemblyMatch = submittedVariant.getModel().isAssemblyMatch();
            boolean evidence = submittedVariant.getModel().isSupportedByEvidence();

            VariantSourceEntry sourceEntry = buildVariantSourceEntry(study, sequenceOntology, validated,
                                                                     submittedVariantValidated, allelesMatch,
                                                                     assemblyMatch, evidence, remappedRS);

            addToVariants(variants, contig, submittedVariantStart, rs, reference, alternate, sourceEntry);
        }
        return new ArrayList<>(variants.values());
    }
    public Map<Long, List<ClusteredVariantEntity>> getAccessionKeyedCVERecords() throws Exception {
        Map<Long, List<ClusteredVariantEntity>> rsHashKeyedCVE = new HashMap<>();
        for (int i = 0; i < this.getChunkSize(); i++) {
            ClusteredVariantEntity cve = this.cursorItemReader.read();
            if (Objects.isNull(cve)) return rsHashKeyedCVE;
            Long rsAccession = cve.getAccession();
            if (!rsHashKeyedCVE.containsKey(rsAccession)) {
                rsHashKeyedCVE.put(rsAccession, new ArrayList<>());
            }
            rsHashKeyedCVE.get(rsAccession).add(cve);
        }
        return rsHashKeyedCVE;
    }

    public Map<ClusteredVariantEntity, List<SubmittedVariantEntity>> getCorrespondingSS(
            Map<Long, List<ClusteredVariantEntity>> accessionKeyedCVEs) {
        List<SubmittedVariantEntity> correspondingSS = new ArrayList<>();
        Map<ClusteredVariantEntity, List<SubmittedVariantEntity>> correspondingRSMap = new HashMap<>();
        Set<Long> rsAccessionsToFind = accessionKeyedCVEs.keySet();

        for (Class<? extends SubmittedVariantEntity> className: Arrays.asList(DbsnpSubmittedVariantEntity.class,
                                                                              SubmittedVariantEntity.class)) {
            correspondingSS.addAll(
             this.mongoTemplate.find(query(where(REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_COLLECTIONS)
                                                   .is(this.assemblyAccession)
                                                   .and(TAXONOMY_FIELD).is(this.taxonomyAccession)
                                                   .and(CLUSTERED_VARIANT_ACCESSION_FIELD).in(rsAccessionsToFind)
                                                   .and(MAPPING_WEIGHT_FIELD).exists(false)),
                                     SubmittedVariantEntity.class, this.mongoTemplate.getCollectionName(className)));
        }
        for (SubmittedVariantEntity sve: correspondingSS) {
            Long rsAccession = sve.getClusteredVariantAccession();
            if (accessionKeyedCVEs.containsKey(rsAccession)) {
                for (ClusteredVariantEntity correspondingCVE: accessionKeyedCVEs.get(rsAccession)) {
                    if (!correspondingRSMap.containsKey(correspondingCVE)) {
                        correspondingRSMap.put(correspondingCVE, new ArrayList<>());
                    }
                    correspondingRSMap.get(correspondingCVE).add(sve);
                }
            }
        }
        return correspondingRSMap;
    }

    @Override
    public List<Variant> read() throws Exception {
        List<Variant> variantList = new ArrayList<>();
        Map<Long, List<ClusteredVariantEntity>> accessionKeyedCVEs = getAccessionKeyedCVERecords();
        if (accessionKeyedCVEs.size() > 0) {
            Map<ClusteredVariantEntity, List<SubmittedVariantEntity>> results = getCorrespondingSS(accessionKeyedCVEs);
            for (Map.Entry<ClusteredVariantEntity, List<SubmittedVariantEntity>> entry : results.entrySet()) {
                variantList.addAll(getVariants(entry.getKey(), entry.getValue()));
            }
        }
        return variantList.size() > 0? variantList: null;
    }

    // The following two overrides are necessary evils to minimize code changes
    // since we still haven't gotten rid of the dependency on VariantAggregationMongoReader
    @Override
    protected List<Bson> buildAggregation() {
        return null;
    }

    @Override
    protected List<Variant> getVariants(Document clusteredVariant) {
        return null;
    }
}
