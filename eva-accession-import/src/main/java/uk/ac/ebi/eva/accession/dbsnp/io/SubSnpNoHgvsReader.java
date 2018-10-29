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

package uk.ac.ebi.eva.accession.dbsnp.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.util.DigestUtils;

import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.ALLELES_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.BATCH_HANDLE_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.BATCH_NAME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.CHROMOSOME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.CHROMOSOME_START_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.CONTIG_NAME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.CONTIG_ORIENTATION_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.CONTIG_START_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.FREQUENCY_EXISTS_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.GENOTYPE_EXISTS_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.LOAD_ORDER_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.REFERENCE_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.RS_CREATE_TIME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.RS_ID_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SNP_CLASS_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SNP_ORIENTATION_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SNP_VALIDATED_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SS_CREATE_TIME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SS_ID_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SUBSNP_ORIENTATION_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.SUBSNP_VALIDATED_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubSnpNoHgvsRowMapper.TAXONOMY_ID_COLUMN;

public class SubSnpNoHgvsReader extends JdbcCursorItemReader<SubSnpNoHgvs> {

    private static final Logger logger = LoggerFactory.getLogger(SubSnpNoHgvsReader.class);

    public SubSnpNoHgvsReader(String assembly, Long buildNumber, DataSource dataSource, int pageSize) throws Exception {
        setDataSource(dataSource);
        setSql(buildSql(assembly, buildNumber));
        setRowMapper(new SubSnpNoHgvsRowMapper(assembly));
        setFetchSize(pageSize);
    }

    @Override
    protected void openCursor(Connection connection) {
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to set autocommit=false", e);
        }
        super.openCursor(connection);
    }

    private String buildSql(String assembly, Long buildNumber) {
        String tableName = getTableName(assembly, buildNumber);
        logger.debug("querying table {} for assembly {}", tableName, assembly);
        String sql =
                "SELECT " +
                        SS_ID_COLUMN +
                        "," + RS_ID_COLUMN +
                        "," + ALLELES_COLUMN +
                        "," + BATCH_HANDLE_COLUMN +
                        "," + BATCH_NAME_COLUMN +
                        "," + CHROMOSOME_COLUMN +
                        "," + CHROMOSOME_START_COLUMN +
                        "," + CONTIG_NAME_COLUMN +
                        "," + SNP_CLASS_COLUMN +
                        "," + SUBSNP_ORIENTATION_COLUMN +
                        "," + SNP_ORIENTATION_COLUMN +
                        "," + CONTIG_ORIENTATION_COLUMN +
                        "," + CONTIG_START_COLUMN +
                        "," + FREQUENCY_EXISTS_COLUMN +
                        "," + GENOTYPE_EXISTS_COLUMN +
                        "," + REFERENCE_COLUMN +
                        "," + SUBSNP_VALIDATED_COLUMN +
                        "," + SNP_VALIDATED_COLUMN +
                        "," + SS_CREATE_TIME_COLUMN +
                        "," + RS_CREATE_TIME_COLUMN +
                        "," + TAXONOMY_ID_COLUMN +
                        " FROM " + tableName +
                        " WHERE rs_id in (136611820, 42568024, 42568025)" +
                        " ORDER BY " + LOAD_ORDER_COLUMN;

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
