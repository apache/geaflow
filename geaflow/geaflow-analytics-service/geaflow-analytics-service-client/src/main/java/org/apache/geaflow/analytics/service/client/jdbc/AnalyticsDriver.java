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

package org.apache.geaflow.analytics.service.client.jdbc;

import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.Integer.parseInt;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.geaflow.analytics.service.client.utils.JDBCUtils;
import org.apache.geaflow.common.exception.GeaflowRuntimeException;

/**
 * This class is an adaptation of Presto's com.facebook.presto.jdbc.PrestoDriver.
 */
public class AnalyticsDriver implements Driver {

    static final String DRIVER_VERSION;
    static final int DRIVER_MAJOR_VERSION;
    static final int DRIVER_MINOR_VERSION;

    static {
        String version = nullToEmpty(AnalyticsDriver.class.getPackage().getImplementationVersion());
        Matcher matcher = Pattern.compile("^(\\d+)\\.(\\d+)($|[.-])").matcher(version);
        if (!matcher.find()) {
            DRIVER_VERSION = "unknown";
            DRIVER_MAJOR_VERSION = 0;
            DRIVER_MINOR_VERSION = 0;
        } else {
            DRIVER_VERSION = version;
            DRIVER_MAJOR_VERSION = parseInt(matcher.group(1));
            DRIVER_MINOR_VERSION = parseInt(matcher.group(2));
        }

        try {
            DriverManager.registerDriver(new AnalyticsDriver());
        } catch (SQLException e) {
            throw new GeaflowRuntimeException("can not register analytics driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties properties) {
        if (!acceptsURL(url)) {
            return null;
        }
        return AnalyticsConnection.newInstance(url, properties);
    }

    @Override
    public boolean acceptsURL(String url) {
        return JDBCUtils.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        AnalyticsDriverURI analyticsDriverURI = new AnalyticsDriverURI(url, info);
        Properties properties = analyticsDriverURI.getProperties();
        ArrayList<DriverPropertyInfo> driverPropertyInfos = new ArrayList<>();
        Set<String> keySets = properties.keySet().stream().map(Object::toString)
            .collect(Collectors.toSet());
        for (String key : keySets) {
            driverPropertyInfos.add(new DriverPropertyInfo(key, properties.getProperty(key)));
        }
        return driverPropertyInfos.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        throw new UnsupportedOperationException();
    }

}
