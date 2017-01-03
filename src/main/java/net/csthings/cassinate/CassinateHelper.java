/*Copyright (c) 2017 Toluwanimi Salako

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */
package net.csthings.cassinate;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

public class CassinateHelper {
    public static final Logger LOG = LoggerFactory.getLogger(CassinateHelper.class);

    public static final String CASSANDRA_SERVERS_KEY = "db.cassandra.servers";
    public static final String CASSANDRA_KEYSPACE_KEY = "db.cassandra.keyspace";

    public static final String CASSANDRA_KEYSPACE_FILE_KEY = "db.cassandra.keyspace.file";
    public static final String CASSANDRA_TABLES_FILE_KEY = "db.cassandra.table.file";
    public static final String CASSANDRA_INDEX_FILE_KEY = "db.cassandra.index.file";
    public static final String CASSANDRA_DATA_FILE_KEY = "db.cassandra.data.file";

    public static final String CASSANDRA_LOCAL_TABLES_FILE = "resources/cassandra/tables.cql";
    public static final String CASSANDRA_LOCAL_DATA_FILE = "resources/cassandra/data.cql";

    protected Cluster cluster;
    protected Session session;

    public CassinateHelper(String... contactPoints) {
        cluster = Cluster.builder().addContactPoints(contactPoints).build();
        session = cluster.connect();
    }

    public CassinateHelper(Cluster cluster) {
        this.cluster = cluster;
        session = cluster.connect();
    }

    public CassinateHelper(Session session) {
        this.session = session;
        cluster = session.getCluster();
    }

    public boolean containsDatabase(String keyspaceName) {
        return cluster.getMetadata().getKeyspace(keyspaceName) != null;
    }

    public boolean containsTable(String keyspaceName, String tableName) {
        return cluster.getMetadata().getKeyspace(keyspaceName).getTable(tableName) != null;
    }

    public void dropDatabase(String keyspaceName) {
        LOG.debug("Dropping keyspace: {}", keyspaceName);
        executeQuery(StringUtils.join("DROP KEYSPACE ", keyspaceName, ";"), session);
    }

    public void dropAllTables(String keyspaceName) {
        Iterator<TableMetadata> tables = cluster.getMetadata().getKeyspace(keyspaceName).getTables().iterator();
        LOG.debug("Truncating tables: {}", tables.toString());
        tables.forEachRemaining(
                t -> session.executeAsync(String.format("DROP TABLE %s.%s ;", keyspaceName, t.getName())));
    }

    public void truncateAllTables(String keyspaceName) {
        Iterator<TableMetadata> tables = cluster.getMetadata().getKeyspace(keyspaceName).getTables().iterator();
        LOG.debug("Truncating tables in {}", keyspaceName);
        tables.forEachRemaining(
                t -> session.executeAsync(String.format("TRUNCATE TABLE %s.%s ;", keyspaceName, t.getName())));
    }

    private static void executeQuery(String query, Session session) {
        try {
            LOG.debug("Executing query:\n===\n{}\n===", query);
            session.execute(query);
        }
        catch (AlreadyExistsException e) {
            LOG.error("Could not execute query", e);
        }
    }

    public void executeQuery(String query) {
        executeQuery(query, session);
    }

    private void executeQueries(List<String> queries, Session session) {
        for (String query : queries) {
            LOG.debug("Executing query in session: -\n {}\n", query);
            executeQuery(query, session);
        }
    }

    public void executeQueriesInKeyspace(String keyspaceName, List<String> queries) {
        Session localSession = cluster.connect(keyspaceName);
        executeQueries(queries, localSession);
    }

    public void executeFile(String filename) throws IOException {
        List<String> queries = getQueries(filename);
        LOG.debug("Found {} queries in file: {}.", queries.size(), filename);
        executeQueries(queries, session);
    }

    public void executeFileInKeyspace(String keyspaceName, String filename) throws IOException {
        Session localSession = cluster.connect(keyspaceName);
        List<String> queries = getQueries(filename);
        LOG.debug("Found {} queries in file: {}.", queries.size(), filename);
        executeQueries(queries, localSession);
    }

    public static List<String> getQueries(String filename) throws IOException {
        File file = new File(filename);
        String query = "";
        List<String> lines = FileUtils.readLines(file, Charset.forName("UTF-8"));
        List<String> queries = new ArrayList<>();
        LOG.debug("Executing file: {}", filename);
        for (String line : lines) {
            if (!StringUtils.isAnyBlank(line)) {
                query = StringUtils.join(query, line);
                continue;
            }
            queries.add(query);
            query = "";
        }
        if (!StringUtils.isAnyBlank(query))
            queries.add(query);

        return queries;
    }

    public int getTableCount(String keyspaceName) {
        return cluster.getMetadata().getKeyspace(keyspaceName).getTables().size();
    }

}
