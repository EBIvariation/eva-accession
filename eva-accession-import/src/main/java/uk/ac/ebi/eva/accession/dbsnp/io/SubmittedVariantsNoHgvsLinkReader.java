/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.util.DigestUtils;

import uk.ac.ebi.eva.accession.dbsnp.model.VariantNoHgvsLink;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.ALLELES_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.BATCH_HANDLE_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.BATCH_NAME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.CHROMOSOME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.CHROMOSOME_START_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.CONTIG_NAME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.CONTIG_ORIENTATION_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.CONTIG_START_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.FREQUENCY_EXIST_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.GENOTYPE_EXIST_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.LOAD_ORDER_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.REFERENCE_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.SS_CREATE_TIME_COLUMN;
import static uk.ac.ebi.eva.accession.dbsnp.io.SubmittedVariantRowMapper.TAXONOMY_ID_COLUMN;

public class SubmittedVariantsNoHgvsLinkReader extends JdbcCursorItemReader<VariantNoHgvsLink> {

    private static final Logger logger = LoggerFactory.getLogger(SubmittedVariantsNoHgvsLinkReader.class);

    public SubmittedVariantsNoHgvsLinkReader(int batch, String assembly, DataSource dataSource,
                                             int pageSize) throws Exception {
        setDataSource(dataSource);
        setSql(buildSql(assembly));
        setPreparedStatementSetter(buildPreparedStatementSetter(batch));
        setRowMapper(new SubmittedVariantRowMapper(assembly));
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

    private String buildSql(String assembly) {
        String tableName = "dbsnp_variant_load_nohgvslink_" + hash(assembly);
        logger.debug("querying table {} for assembly {}", tableName, assembly);
        String sql =
                "SELECT " +
                        ALLELES_COLUMN +
                        "," + BATCH_HANDLE_COLUMN +
                        "," + BATCH_NAME_COLUMN +
                        "," + CHROMOSOME_COLUMN +
                        "," + CHROMOSOME_START_COLUMN +
                        "," + CONTIG_NAME_COLUMN +
                        "," + CONTIG_ORIENTATION_COLUMN +
                        "," + CONTIG_START_COLUMN +
                        "," + FREQUENCY_EXIST_COLUMN +
                        "," + GENOTYPE_EXIST_COLUMN +
                        "," + LOAD_ORDER_COLUMN +
                        "," + REFERENCE_COLUMN +
                        "," + SS_CREATE_TIME_COLUMN +
                        "," + TAXONOMY_ID_COLUMN +
                        " FROM " + tableName +
                        " WHERE batch_id = ? " +
                        " ORDER BY " + LOAD_ORDER_COLUMN;

        return sql;
    }

    private String hash(String string) {
        return DigestUtils.md5DigestAsHex(string.getBytes());
    }

    private PreparedStatementSetter buildPreparedStatementSetter(int batch) {
        PreparedStatementSetter preparedStatementSetter = new ArgumentPreparedStatementSetter(
                new Object[]{batch}
        );
        return preparedStatementSetter;
    }

}
