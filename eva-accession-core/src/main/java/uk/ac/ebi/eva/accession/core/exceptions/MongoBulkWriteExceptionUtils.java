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
package uk.ac.ebi.eva.accession.core.exceptions;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Common utility class to assist in handling Mongo DB MongoBulkWriteException
 */
public class MongoBulkWriteExceptionUtils {

    private static final String DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME = "IDKEY";

    private static final String DUPLICATE_KEY_ERROR_MESSAGE_REGEX = "index:\\s.*\\{\\s?.*\\:\\s?\"(?<"
            + DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME + ">[a-zA-Z0-9_.]+)\"\\s?\\}";

    private static final Pattern DUPLICATE_KEY_PATTERN = Pattern.compile(DUPLICATE_KEY_ERROR_MESSAGE_REGEX);

    public static Stream<String> extractUniqueHashesForDuplicateKeyError(MongoBulkWriteException exception) {
        return exception.getWriteErrors()
                        .stream()
                        .filter(MongoBulkWriteExceptionUtils::isDuplicateKeyError)
                        .map(error -> {
                            Matcher matcher = DUPLICATE_KEY_PATTERN.matcher(error.getMessage());
                            if (matcher.find()) {
                                String hash = matcher.group(DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME);
                                if (hash == null) {
                                    throw new IllegalStateException(
                                        "A duplicate key exception was caught, but the message couldn't be " +
                                                "parsed correctly. The group in the regex " +
                                                DUPLICATE_KEY_ERROR_MESSAGE_REGEX + " failed to match part of" +
                                                " the input",
                                        exception);
                                }
                                return hash;
                            } else {
                                throw new IllegalStateException("A duplicate key exception was caught, but the " +
                                            "message couldn't be parsed correctly",
                                    exception);
                            }
                        })
                        .distinct();
    }

    private static boolean isDuplicateKeyError(BulkWriteError error) {
        ErrorCategory errorCategory = ErrorCategory.fromErrorCode(error.getCode());
        return errorCategory.equals(ErrorCategory.DUPLICATE_KEY);
    }
}
