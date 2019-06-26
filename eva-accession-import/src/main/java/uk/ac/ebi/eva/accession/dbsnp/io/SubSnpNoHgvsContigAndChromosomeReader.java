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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.util.DigestUtils;

import uk.ac.ebi.eva.accession.dbsnp.model.CoordinatesPresence;

import javax.sql.DataSource;

/**
 * This Spring Batch reader obtains the list of distinct contigs in a dbSNP database.
 */
public class SubSnpNoHgvsContigAndChromosomeReader extends JdbcCursorItemReader<CoordinatesPresence> {

    static final String CONTIG_NAME_COLUMN = "contig_name";

    static final String CHROMOSOME_NAME_COLUMN = "chromosome";

    static final String CHROMOSOME_START_PRESENT_COLUMN = "chromosome_start_present";

    private static final Logger logger = LoggerFactory.getLogger(SubSnpNoHgvsContigAndChromosomeReader.class);

    public SubSnpNoHgvsContigAndChromosomeReader(String assembly, Long buildNumber, DataSource dataSource,
                                                 int pageSize) throws Exception {
        setDataSource(dataSource);
        setSql(buildSql(assembly, buildNumber));
        setRowMapper(new SubSnpNoHgvsContigAndChromosomeRowMapper());
        setFetchSize(pageSize);
    }

    private String buildSql(String assembly, Long buildNumber) {
        String tableName = getTableName(assembly, buildNumber);
        logger.debug("querying table {} for assembly {}", tableName, assembly);
        String sql = "SELECT DISTINCT " + CONTIG_NAME_COLUMN
                     + ", " + CHROMOSOME_NAME_COLUMN
                     + ", chromosome_start is not null as "  + CHROMOSOME_START_PRESENT_COLUMN
                     + " FROM " + tableName;
        return sql;
    }

    private String getTableName(String assembly, Long buildNumber) {
        if (buildNumber == null) {
            return "dbsnp_variant_load_nohgvslink_" + hash(assembly);
        } else {
            return "dbsnp_nohgvs_" + hash(assembly) + "_b" + buildNumber;
        }
    }

    private String hash(String string) {
        return DigestUtils.md5DigestAsHex(string.getBytes());
    }
}
