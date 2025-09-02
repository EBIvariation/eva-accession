package uk.ac.ebi.eva.accession.release.batch.io;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import static uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader.ACCESSION_FIELD;

public class DumpRSAccessionsInFile {
    private static Logger logger = LoggerFactory.getLogger(DumpRSAccessionsInFile.class);
    public static final String CVE_ASSEMBLY_FIELD = "asm";
    public static final String CVE_OPS_EVENT_TYPE_FIELD = "eventType";
    public static final String CVE_OPS_INACTIVE_OBJ_ASSEMBLY_FIELD = "inactiveObjects.asm";

    private MongoTemplate mongoTemplate;
    private String rsAccDumpFile;
    private int chunkSize;

    public DumpRSAccessionsInFile(MongoTemplate mongoTemplate, String rsAccDumpFile, int chunkSize) {
        this.mongoTemplate = mongoTemplate;
        this.rsAccDumpFile = rsAccDumpFile;
        this.chunkSize = chunkSize;
    }

    public void dumpAccessions(RSDumpType rsDumpType, String assembly) {
        Bson query = getQueryforRSDumpType(rsDumpType, assembly);

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(rsAccDumpFile, true))) {
            logger.info("Issuing find in EVA collection: {}", query);
            FindIterable<Document> clusteredVariants = getClusteredVariants(query, rsDumpType.getEvaClass());
            MongoCursor<Document> evaCursor = clusteredVariants.iterator();
            writeDataToFile(evaCursor, bufferedWriter);

            evaCursor.close();

            logger.info("Issuing find in DBSNP collection: {}", query);
            FindIterable<Document> dbsnpClusteredVariants = getClusteredVariants(query, rsDumpType.getDbsnpClass());
            MongoCursor<Document> dbsnpCursor = dbsnpClusteredVariants.iterator();
            writeDataToFile(dbsnpCursor, bufferedWriter);

            dbsnpCursor.close();

            logger.info("Data Written to file successfully: {}", query);
        } catch (IOException e) {
            logger.error("Error dumping rs accessions to file: {}", rsAccDumpFile, e);
        }
    }

    private FindIterable<Document> getClusteredVariants(Bson query, Class<?> entityClass) {
        return mongoTemplate.getCollection(mongoTemplate.getCollectionName(entityClass))
                .find(query)
                .noCursorTimeout(true)
                .batchSize(chunkSize);
    }

    private void writeDataToFile(MongoCursor<Document> cursor, BufferedWriter bufferedWriter) throws IOException {
        StringBuilder batch = new StringBuilder();
        int totalDocuments = 0;
        int count = 0;

        while (cursor.hasNext()) {
            Document doc = cursor.next();
            Object accession = doc.get(ACCESSION_FIELD);
            if (accession != null) {
                batch.append(accession).append("\n");
                count++;
            }

            if (count >= chunkSize) {
                bufferedWriter.write(batch.toString());
                totalDocuments += count;
                if (totalDocuments % 100000 == 0) {
                    logger.info("Total document written till now: {}", totalDocuments);
                }
                batch.setLength(0);
                count = 0;
            }
        }

        if (batch.length() > 0) {
            bufferedWriter.write(batch.toString());
            totalDocuments += count;
            logger.info("Total document written till now: {}", totalDocuments);
        }
    }

    public Bson getQueryforRSDumpType(RSDumpType rsDumpType, String assembly) {
        if (rsDumpType == RSDumpType.ACTIVE) {
            return Filters.eq(CVE_ASSEMBLY_FIELD, assembly);
        } else if (rsDumpType == RSDumpType.MERGED_AND_DEPRECATED) {
            return Filters.and(Filters.eq(CVE_OPS_INACTIVE_OBJ_ASSEMBLY_FIELD, assembly),
                    Filters.in(CVE_OPS_EVENT_TYPE_FIELD, EventType.MERGED.toString(), EventType.DEPRECATED.toString()));
        }

        return null;
    }


    public enum RSDumpType {
        ACTIVE(ClusteredVariantEntity.class, DbsnpClusteredVariantEntity.class),
        MERGED_AND_DEPRECATED(ClusteredVariantOperationEntity.class, DbsnpClusteredVariantOperationEntity.class);

        private Class evaClass;
        private Class dbsnpClass;

        RSDumpType(Class evaClass, Class dbsnpClass) {
            this.evaClass = evaClass;
            this.dbsnpClass = dbsnpClass;
        }

        public Class getEvaClass() {
            return evaClass;
        }

        public Class getDbsnpClass() {
            return dbsnpClass;
        }
    }

}
