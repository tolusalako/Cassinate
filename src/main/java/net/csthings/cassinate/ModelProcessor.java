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

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.inject.internal.MoreTypes.ParameterizedTypeImpl;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ClassAnnotationMatchProcessor;

/**
 * Created on: Jan 2, 2017
 * @author Toluwanimi Salako
 * Last edited: Jan 2, 2017
 * @purpose - A processor class that's called whenever a class matching an anotation has been found
 */

public class ModelProcessor implements ClassAnnotationMatchProcessor {
    public static final Logger LOG = LoggerFactory.getLogger(ModelProcessor.class);
    public static final Class<?> annotation = Table.class;
    /**
     * Mappings for cassandra java types
     * http://docs.datastax.com/en/developer/java-driver/3.1/manual/
     */
    private static final Map<Class<?>, DataType> mappings;
    private static final Map<ParameterizedType, CollectionType> collectionMappings;

    static {
        mappings = new HashMap<>();
        mappings.put(String.class, DataType.text());
        mappings.put(long.class, DataType.bigint());
        mappings.put(ByteBuffer.class, DataType.blob());
        mappings.put(int.class, DataType.cint());
        mappings.put(boolean.class, DataType.cboolean());
        mappings.put(Date.class, DataType.timestamp());
        mappings.put(double.class, DataType.cdouble());
        mappings.put(InetAddress.class, DataType.inet());
        mappings.put(UUID.class, DataType.timeuuid());

        collectionMappings = new HashMap<>();
        try {
            for (Class<?> c : mappings.keySet()) {
                collectionMappings.put(
                        new ParameterizedTypeImpl(null, List.class, c.isPrimitive() ? getWrapperType(c) : c),
                        DataType.list(mappings.get(c)));
                collectionMappings.put(
                        new ParameterizedTypeImpl(null, Set.class, c.isPrimitive() ? getWrapperType(c) : c),
                        DataType.set(mappings.get(c)));
                // TODO map ?? Construct in a function
            }
        }
        catch (IllegalArgumentException e) {
            LOG.error("Could not init", e);
        }
    }

    private List<Model> models;
    private boolean ignoreSubclasses;

    private List<Class<?>> annotationIgnoreList;
    protected int modifierToIgnore;

    public ModelProcessor() {
        models = new ArrayList<>();
        annotationIgnoreList = new ArrayList<>();
    }

    /**
     * Processes the class {@link clazz} into a  {@link Model} and adds it to {@link models}
     * This is automatically called after {@link FastClasspathScanner#scan()}
     * @param clazz Class matching the annotation provided to {@link FastClasspathScanner#matchClassesWithAnnotation}
     */
    @Override
    public void processMatch(Class<?> clazz) {
        Class<?> parent = clazz.getDeclaringClass();
        if (null != parent && ignoreSubclasses) {
            // Skip sub classes
            return;
        }

        try {
            Table table = clazz.getDeclaredAnnotation(Table.class);
            if (table == null) {
                // Skip classes without the table annotation
                return;
            }
            String name = table.name();
            if (name.isEmpty())
                name = clazz.getName();

            Model model = new Model();
            model.setName(name);
            LOG.debug("Processing {}", name);

            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                int mods = field.getModifiers();
                if ((modifierToIgnore & mods) != 0) {
                    LOG.debug("Skipping ignored field: {}", field.getName());
                    continue;
                }

                String fieldName = field.getName().toLowerCase();
                Column col = field.getDeclaredAnnotation(Column.class);
                if (col != null && !col.name().isEmpty())
                    fieldName = col.name();

                DataType type = mappings.get(field.getType());
                if (type == null)
                    type = collectionMappings.get(field.getGenericType());

                model.getColumns().put(fieldName, type);
                boolean isPrimKey = field.getAnnotation(PartitionKey.class) != null;
                boolean isClusKey = field.getAnnotation(ClusteringColumn.class) != null;
                if (isPrimKey || isClusKey) {
                    model.getPrimaryKeys().add(field.getName());
                }
            }
            models.add(model);
        }
        catch (Exception e) {
            LOG.error("Could not load class: {}", clazz.getName(), e);
            return;
        }
    }

    /**
     * List of processed {@link Model}s
     * @return
     */
    public List<Model> getModels() {
        return models;
    }

    public boolean isIgnoreSubclasses() {
        return ignoreSubclasses;
    }

    public void setIgnoreSubclasses(boolean ignoreSubclasses) {
        this.ignoreSubclasses = ignoreSubclasses;
    }

    public List<Class<?>> getAnnotationIgnoreList() {
        return annotationIgnoreList;
    }

    public void setAnnotationIgnoreList(List<Class<?>> annotationIgnoreList) {
        this.annotationIgnoreList = annotationIgnoreList;
    }

    /**
     * Returns the wrapper type of {@link type} if it is primitive else returns{@link type}
     * @param type
     * @return
     */
    private static Class<?> getWrapperType(Class<?> type) {
        if (!type.isPrimitive())
            return type;

        if (type == double.class)
            return Double.class;
        else if (type == float.class)
            return Float.class;
        else if (type == long.class)
            return Long.class;
        else if (type == int.class)
            return Integer.class;
        else if (type == short.class)
            return Short.class;
        else if (type == char.class)
            return Character.class;
        else if (type == byte.class)
            return Byte.class;
        else if (type == boolean.class)
            return Boolean.class;
        else
            return null;
    }
}
