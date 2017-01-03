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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;

/**
 * Created on: Jan 2, 2017
 * @author Toluwanimi Salako
 * Last edited: Jan 2, 2017
 * @purpose -
 * <p/>
 * This is the main entry point for Cassinate. An example use would be
 * <pre>
 *   Cassinate cassinate = Cassinate.builder().contactPoint("192.168.0.1").ignoreAnnotation(JsonIgnore.class).build();
 * </pre>
 */
public class Cassinate {
    public static final Logger LOG = LoggerFactory.getLogger(Cassinate.class);

    private String keyspaceName;
    private CassinateHelper helper;

    private Cassinate(String keyspaceName, ModelProcessor mp, CassinateHelper helper) {
        this.keyspaceName = keyspaceName;
        this.helper = helper;
        FastClasspathScanner scanner = new FastClasspathScanner();
        FastClasspathScanner result = scanner.matchClassesWithAnnotation(ModelProcessor.annotation, mp);
        result.scan();

        List<Model> models = mp.getModels();
        validateModels(models);
    }

    /**
     * @purpose - Builder class for {@link Cassinate}
     */
    public static class Builder {
        private ModelProcessor mp = new ModelProcessor();
        private String keyspaceName;
        private Cluster cluster;
        private Session session;
        private List<String> contactPoints = new ArrayList<>();

        /**
         * Sets the keyspace for Cassandra to connect to
         * @param keyspaceName
         * @return
         */
        public Builder useKeyspace(String keyspaceName) {
            this.keyspaceName = keyspaceName;
            return this;
        }

        /**
         * Adds a new contact point for the Cassandra {@link Cluster}.
         * <p/>
         * See {@link Builder#addContactPoint}
         * <p/>
         * Overridden by ({@link #useCluster}, {@link #useSession})
         * @param contactPoint
         * @return
         */
        public Builder addContactPoint(String contactPoint) {
            contactPoints.add(contactPoint);
            return this;
        }

        /**
         * Sets the {@link Cluster} for Cassandra.
         * <p/>
         * Overridden by ({@link #useSession})
         * <p/>
         * Overrides ({@link #addContactPoint})
         * @param contactPoint
         * @return
         */
        public Builder useCluster(Cluster cluster) {
            this.cluster = cluster;
            return this;
        }

        /**
         * Sets the {@link Session} for Cassandra.
         * <p/>
         * Overrides ({@link #useCluster}, {@link #addContactPoint})
         * @param contactPoint
         * @return
         */
        public Builder useSession(Session session) {
            this.session = session;
            return this;
        }

        /**
         * Sets the rule for Cassinate to ignore any subclass
         * @return the updated {@link Builder}
         */
        public Builder ignoreSubclasses() {
            this.mp.setIgnoreSubclasses(true);
            return this;
        }

        /**
         * Adds a {@link Class} for Cassinate to ignore
         * @param annotation the annotation class to ignore
         * @return the updated {@link Builder}
         */
        public Builder ignoreAnnotation(Class<?> annotation) {
            this.mp.getAnnotationIgnoreList().add(annotation);
            return this;
        }

        /**
         * Adds a {@link java.lang.reflect.Modifier} for Cassinate to ignore
         * For example to ignore all {@code final} fields:
         * <pre>
         *   Cassinate.Builder builder = Cassinate.builder();
         *   builder.ignoreModifier({@link Modifier#Final});
         * </pre>
         * @param mod Modifier to ignore
         * @return the updated {@link Builder}
         */
        public Builder ignoreModifier(int mod) {
            this.mp.modifierToIgnore += mod;
            return this;
        }

        /**
         * Builds a new {@link Cassinate} instance from the preconfigured settings
         * @return the new instance
         */
        public Cassinate build() {
            CassinateHelper helper;
            if (null != session) {
                helper = new CassinateHelper(session);
            }
            else if (null != cluster) {
                helper = new CassinateHelper(cluster);
            }
            else {
                helper = new CassinateHelper(contactPoints.toArray(new String[contactPoints.size()]));
            }
            return new Cassinate(keyspaceName, mp, helper);
        }
    }

    /**
     * Creates a new {@link Cassinate.Builder} instance.
     * @return the new cassinate builder.
     */
    public static Cassinate.Builder builder() {
        return new Cassinate.Builder();
    }

    /**
     * Ensures that each {@link Model} in {@link models} exists and has the same {@link DataType}.
     * @param models Models to validate
     */
    private void validateModels(List<Model> models) {
        KeyspaceMetadata km = helper.cluster.getMetadata().getKeyspace(keyspaceName);
        List<String> queriesToExecute = new ArrayList<>();

        for (Model model : models) {
            TableMetadata table = km.getTable(model.getName());
            if (null == table) {
                queriesToExecute.add(createModel(model));
                continue;
            }
            Map<String, ? super DataType> modelCols = new HashMap<>(model.getColumns());
            Iterator<ColumnMetadata> columnsIterator = table.getColumns().iterator();

            List<ColumnMetadata> keys = table.getPrimaryKey();
            // TODO what if primary key was changed? Drop all and recreate

            while (columnsIterator.hasNext()) {
                ColumnMetadata next = columnsIterator.next();
                DataType type = null;

                try {
                    type = DataType.class.cast(modelCols.get(next.getName()));
                }
                catch (ClassCastException e) {
                    LOG.debug("Could not cast to Datatype. Tying Collection.", e);
                    type = CollectionType.class.cast(modelCols.get(next.getName()));
                }

                if (null == type) {
                    // Column has been deleted from model. Drop it.
                    queriesToExecute.add(String.format("ALTER TABLE %s DROP %s;", model.getName(), next.getName()));
                }
                else if (type.getName().equals(next.getType().getName())) {
                    // Everything matches
                    modelCols.remove(next.getName());
                }
                else {
                    // Column type needs to be changed
                    queriesToExecute.add(String.format("ALTER TABLE %s ALTER %s TYPE %s;", model.getName(),
                            next.getName(), modelCols.get(next.getName())));
                }
            }

            // Create the remaining
            for (Entry<String, ? super DataType> e : modelCols.entrySet()) {
                // Alter tables
                // https://docs.datastax.com/en/cql/3.1/cql/cql_reference/alter_table_r.html#reference_ds_xqq_hpc_xj__adding-a-column
                if (e.getValue() instanceof CollectionType) {
                    CollectionType ct = (CollectionType) e.getValue();
                    StringBuilder sb = new StringBuilder();
                    sb.append(ct.getName().name() + "<");
                    for (DataType dt : ct.getTypeArguments()) {
                        sb.append(dt.getName().name() + ", ");
                    }
                    sb.replace(sb.length() - 2, sb.length(), ">");

                    queriesToExecute.add(
                            String.format("ALTER TABLE %s ADD %s %s;", model.getName(), e.getKey(), sb.toString()));
                }
                else {
                    queriesToExecute.add(String.format("ALTER TABLE %s ADD %s %s;", model.getName(), e.getKey(),
                            ((DataType) e.getValue()).getName()));
                }

            }
        }
        helper.executeQueriesInKeyspace(keyspaceName, queriesToExecute);

    }

    /**
     * Generates the cql query for Tables that weren't found
     * @param model
     * @return
     */
    private String createModel(Model model) {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(String.format("CREATE TABLE IF NOT EXISTS %s (\n", model.getName()));
        for (Entry<String, ? super DataType> e : model.getColumns().entrySet()) {
            queryBuilder.append(String.format("%s %s,%s", e.getKey(), ((DataType) e.getValue()).getName(), "\n"));
        }
        queryBuilder.append("PRIMARY KEY (");
        for (String key : model.getPrimaryKeys()) {
            queryBuilder.append(key + ", ");
        }
        queryBuilder.replace(queryBuilder.length() - 2, queryBuilder.length(), ")\n);");
        return queryBuilder.toString();
    }
}
