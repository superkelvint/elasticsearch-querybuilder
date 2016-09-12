package com.adamantite.es.querybuilder;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.common.hppc.ObjectFloatOpenHashMap;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

public class QueryDSLType {
    public static enum TYPE {
        FILTER, QUERY
    }
    public static enum PARAM_TYPE {
        BOOLEAN, STRING, INTEGER, FLOAT, DOUBLE, OBJECT,
        LIST, MAP, FUZZINESS, OPERATOR, QUERY_BUILDER, FILTER_BUILDER,
        OTHER, LIST_QUERY_BUILDER, LIST_FILTER_BUILDER,
        GEO_DISTANCE, SHAPE_BUILDER, SPATIAL_STRATEGY, SHAPE_RELATION,
        OPTIMIZE_BBOX
    }

    public static class Param {
        PARAM_TYPE type;
        String paramName;
        String label;

        public Param(PARAM_TYPE type) {
            this.type = type;
        }

        public Param(PARAM_TYPE type, String paramName) {
            this.type = type;
            this.paramName = paramName;
        }

        public Param(PARAM_TYPE type, String paramName, String label) {
            this.type = type;
            this.paramName = paramName;
            this.label = label;
        }
    }

    /**
     * Name of this dsl type. Usually the value found in xxxParser.NAME
     */
    final String dslName;

    final TYPE type;

    /**
     * Does this dsl type depend on a named field?
     * Examples of one which does is CommonTermsQueryBuilder
     */
    boolean namedObject = false;

    boolean namedObjectValue = false;

    /**
     * Does this dsl type depend on a named array?
     * At the moment, only TermsQueryBuilder,
     * GeoDistanceFilterBuilder and GeoDistanceRangeFilterBuilder uses this.
     */
    boolean namedArray = false;

    /**
     * Comma-separated values for namedArray.
     * At the moment, only GeoDistanceFilterBuilder and GeoDistanceRangeFilterBuilder uses this.
     * The code looks like this: builder.startArray(name).value(lon).value(lat).endArray();
     */
    String namedArrayValues;

    /**
     * The fields of the DSL type obtained via java Reflection.
     *
     * There's a difference between the field name and the dsl param name.
     * The field name is often camel-case, whereas the dsl param name is often underscored.
     */
    Map<String, Param> fields = new TreeMap<String, Param>();

    public QueryDSLType(String dslName, TYPE type) {
        this.dslName = dslName;
        this.type = type;
    }

    public void addField(Field field) {
        Class type = field.getType();
        String fieldName = field.getName();
        Type genericType = field.getGenericType();
        addField(type, fieldName, genericType);
    }

    protected void addField(Class type, String fieldName, Type genericType) {
        if (type.equals(String.class)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.STRING));
        } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.BOOLEAN));
        } else if (type.equals(Float.class) || type.equals(Float.TYPE)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.FLOAT));
        } else if (type.equals(Double.class) || type.equals(Double.TYPE)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.DOUBLE));
        } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.INTEGER));
        } else if (type.equals(QueryBuilder.class) || type.getName().endsWith("QueryBuilder")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.QUERY_BUILDER));
            this.setParamName(fieldName, "query");
        } else if (type.equals(FilterBuilder.class) || type.getName().endsWith("FilterBuilder")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.FILTER_BUILDER));
            this.setParamName(fieldName, "filter");
        } else if (type.equals(Fuzziness.class)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.FUZZINESS));
            this.setParamName(fieldName, "fuzziness");
        } else if (type.equals(Object.class)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.OBJECT));
        } else if (type.getName().contains("Operator")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.OPERATOR));
        } else if (type.getName().endsWith("GeoDistance")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.GEO_DISTANCE));
        } else if (type.getName().endsWith("SpatialStrategy")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.SPATIAL_STRATEGY));
        } else if (type.getName().endsWith("ShapeBuilder")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.SHAPE_BUILDER));
        } else if (type.getName().endsWith("ShapeRelation")) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.SHAPE_RELATION));
        } else if (type.getName().startsWith("[L") || type.equals(List.class) || type.equals(ArrayList.class)) {
            PARAM_TYPE _type = PARAM_TYPE.LIST;
            if(genericType != null && genericType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) genericType;
                genericType = pt.getActualTypeArguments()[0];
                if(genericType.equals(QueryBuilder.class)) {
                    _type = PARAM_TYPE.LIST_QUERY_BUILDER;
                } else if(genericType.equals(FilterBuilder.class)) {
                    _type = PARAM_TYPE.LIST_FILTER_BUILDER;
                }
            }
            this.fields.put(fieldName, new Param(_type));
            this.setParamName(fieldName, fieldName);
        } else if (type.equals(Map.class)) {
            this.fields.put(fieldName, new Param(PARAM_TYPE.MAP));
        } else if (!type.equals(ObjectFloatOpenHashMap.class)) {
            System.out.println("Unknown field type: " + fieldName + " " + type.getName() + " on " + dslName);
            this.fields.put(fieldName, new Param(PARAM_TYPE.OTHER));
        }
    }

    public void setParamName(String name, String param) {
        final Param p = this.fields.get(name);
        if (p == null) {
            System.out.println("Unknown field: " + name + " on " + this.dslName);
        } else {
            p.paramName = param;
        }
    }

    public boolean isNamedObject() {
        return namedObject;
    }

    public void setNamedObject(boolean namedObject) {
        this.namedObject = namedObject;
    }

    public boolean isNamedObjectValue() {
        return namedObjectValue;
    }

    public void setNamedObjectValue(boolean namedObjectValue) {
        this.namedObjectValue = namedObjectValue;
    }

    public boolean isNamedArray() {
        return namedArray;
    }

    public void setNamedArray(boolean namedArray) {
        this.namedArray = namedArray;
    }

    public String getNamedArrayValues() {
        return namedArrayValues;
    }

    public void setNamedArrayValues(String namedArrayValues) {
        this.namedArrayValues = namedArrayValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryDSLType that = (QueryDSLType) o;

        if (!dslName.equals(that.dslName)) return false;
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dslName.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override public String toString() {
        final StringBuilder sb = new StringBuilder("QueryDSLType{");
        sb.append("dslName='").append(dslName).append('\'');
        sb.append(", type=").append(type);
        sb.append(", fields=").append(fields);
        sb.append('}');
        return sb.toString();
    }
}
