package net.csthings.cassinate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataType;

/**
 * Created on: Jan 2, 2017
 * @author Toluwanimi Salako
 * Last edited: Jan 2, 2017
 * @purpose - Model class to represent cassandra tables
 */
public class Model {
    private String name;
    private Map<String, ? super DataType> columns;
    private List<String> primaryKeys;

    public Model() {
        columns = new HashMap<>();
        primaryKeys = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, ? super DataType> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, ? super DataType> columns) {
        this.columns = columns;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    @Override
    public String toString() {
        return "Model [name=" + name + ", columns=" + columns + ", primaryKeys=" + primaryKeys + "]";
    }

}
