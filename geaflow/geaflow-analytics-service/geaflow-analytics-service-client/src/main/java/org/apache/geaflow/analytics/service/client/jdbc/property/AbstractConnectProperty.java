/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geaflow.analytics.service.client.jdbc.property;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is an adaptation of Presto's com.facebook.presto.jdbc.AbstractConnectionProperty.
 */
public abstract class AbstractConnectProperty<T> implements ConnectProperty<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectProperty.class);
    private static final String TRUE_FLAG = "true";
    private static final String FALSE_FLAG = "false";

    private final String key;
    private final Optional<String> defaultValue;
    private final Predicate<Properties> enableRequired;
    private final Predicate<Properties> enableAllowed;
    private final Converter<T> converter;

    protected AbstractConnectProperty(String key, Optional<String> defaultValue,
                                      Predicate<Properties> enableRequired,
                                      Predicate<Properties> enableAllowed,
                                      Converter<T> converter) {
        this.key = requireNonNull(key, "key is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        this.enableRequired = requireNonNull(enableRequired, "enableRequired is null");
        this.enableAllowed = requireNonNull(enableAllowed, "enableAllowed is null");
        this.converter = requireNonNull(converter, "converter is null");
    }

    protected AbstractConnectProperty(
        String key,
        Predicate<Properties> required,
        Predicate<Properties> allowed,
        Converter<T> converter) {
        this(key, Optional.empty(), required, allowed, converter);
    }

    @Override
    public String getPropertyKey() {
        return key;
    }

    @Override
    public Optional<String> getDefault() {
        return defaultValue;
    }

    @Override
    public boolean enableRequired(Properties properties) {
        return enableRequired.test(properties);
    }

    @Override
    public boolean enableAllowed(Properties properties) {
        return !properties.containsKey(key) || enableAllowed.test(properties);
    }

    @Override
    public Optional<T> getValue(Properties properties) throws GeaflowRuntimeException {
        String value = properties.getProperty(key);
        if (value == null) {
            if (enableRequired(properties)) {
                throw new GeaflowRuntimeException(format("Connection property '%s' is required", key));
            }
            return Optional.empty();
        }

        try {
            return Optional.of(converter.convert(value));
        } catch (RuntimeException e) {
            if (value.isEmpty()) {
                throw new GeaflowRuntimeException(format("Connection property '%s' value is empty", key), e);
            }
            throw new GeaflowRuntimeException(
                format("Connection property '%s' value is invalid: %s", key, value), e);
        }
    }

    @Override
    public void validate(Properties properties)
        throws GeaflowRuntimeException {
        if (!enableAllowed(properties)) {
            throw new GeaflowRuntimeException(format("Connection property '%s' is not allowed", key));
        }

        getValue(properties);
    }

    protected static final Predicate<Properties> REQUIRED = properties -> true;
    protected static final Predicate<Properties> NOT_REQUIRED = properties -> false;

    protected static final Predicate<Properties> ALLOWED = properties -> true;

    interface Converter<T> {

        T convert(String value);
    }

    protected static final Converter<String> STRING_CONVERTER = value -> value;

    protected static final Converter<String> NON_EMPTY_STRING_CONVERTER = value -> {
        checkArgument(!value.isEmpty(), "value is empty");
        return value;
    };

    protected static final Converter<Boolean> BOOLEAN_CONVERTER = value -> {
        switch (value.toLowerCase(ENGLISH)) {
            case TRUE_FLAG:
                return true;
            case FALSE_FLAG:
                return false;
            default:
                break;
        }
        throw new IllegalArgumentException("value must be 'true' or 'false'");
    };

    protected static final class StringMapConverter implements Converter<Map<String, String>> {

        private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E);

        public static final StringMapConverter STRING_MAP_CONVERTER = new StringMapConverter();

        private static final char DELIMITER_COLON = ':';

        private static final char DELIMITER_SEMICOLON = ';';

        private StringMapConverter() {

        }

        @Override
        public Map<String, String> convert(String value) {
            return Splitter.on(DELIMITER_SEMICOLON).splitToList(value).stream()
                .map(this::parseKeyValuePair)
                .collect(Collectors.toMap(entry -> entry.get(0), entry -> entry.get(1)));
        }

        public List<String> parseKeyValuePair(String keyValue) {
            List<String> nameValue = Splitter.on(DELIMITER_COLON).splitToList(keyValue);
            checkArgument(nameValue.size() == 2, "Malformed key value pair: %s", keyValue);
            String name = nameValue.get(0);
            String value = nameValue.get(1);
            checkArgument(!name.isEmpty(), "Key is empty");
            checkArgument(!value.isEmpty(), "Value is empty");

            checkArgument(PRINTABLE_ASCII.matchesAllOf(name),
                "Key contains spaces or is not printable ASCII: %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value),
                "Value contains spaces or is not printable ASCII: %s", name);
            return nameValue;
        }
    }

    protected interface CheckedPredicate<T> {
        boolean test(T t) throws GeaflowRuntimeException;
    }

    protected static <T> Predicate<T> checkedPredicate(CheckedPredicate<T> predicate) {
        return t -> {
            try {
                return predicate.test(t);
            } catch (GeaflowRuntimeException e) {
                LOGGER.warn("check predicate error", e);
                return false;
            }
        };
    }
}
