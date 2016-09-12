package com.adamantite.es.querybuilder;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;

/**
 * Usage: QueryDSLTypeExtractor -src [elasticsearch src dir] -out [output dir] -version [es version]
 */
public class QueryDSLTypeExtractor {
    public static String esVersion = "1.7.2";
    public static String esSrcHome = "/home/kelvin/java/search/elasticsearch-"+ esVersion +"-src";
    public static String esQuerybuilderDir = esSrcHome + "/src/main/java/org/elasticsearch/index/query";
    public static ClassLoader classLoader;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String outputDir = "/tmp";
        for (int i = 0, n = args.length; i < n; i++) {
            if(args[i].equals("-src")) {
                esSrcHome = args[++i];
            }
            if(args[i].equals("-version")) {
                esVersion = args[++i];
            }
            if(args[i].equals("-out")) {
                outputDir = args[++i];
            }
        }
        init();
        Map<String, QueryDSLType> queries = QueryDSLTypeExtractor.parseQueries(esQuerybuilderDir, classLoader);
        Map<String, QueryDSLType> filters = QueryDSLTypeExtractor.parseFilters(esQuerybuilderDir, classLoader);
        String json = QueryDSLTypeExtractor.toJSON(queries, filters, false, true);
        BufferedWriter writer = new BufferedWriter(new FileWriter(outputDir + "/qb-model-"+ esVersion +".json"));
        writer.write(json);
        writer.close();
    }

    public static void init() throws MalformedURLException {
        ArrayList<URL> files = new ArrayList<URL>();
        for(File f: new File(esSrcHome).listFiles()) {
            if(f.getName().endsWith(".jar")) {
                files.add(f.toURL());
            }
        }

        classLoader = new URLClassLoader(files.toArray(new URL[files.size()]), Thread.currentThread().getContextClassLoader());
    }

    public static Map<String, QueryDSLType> parseQueries(String baseDir, ClassLoader classLoader) throws ClassNotFoundException {
        Map<String, QueryDSLType> result = new TreeMap<String, QueryDSLType>();
        File dir = new File(baseDir);
        parse(result, dir, QueryDSLType.TYPE.QUERY, classLoader);
        applyQueryBuilderWorkarounds(result);
        return result;
    }

    protected static void applyQueryBuilderWorkarounds(Map<String, QueryDSLType> result) {
        QueryDSLType common = result.get("common");
        common.fields.get("lowFreqMinimumShouldMatch").paramName = "minimum_should_match." + common.fields.get("lowFreqMinimumShouldMatch").paramName;
        common.fields.get("highFreqMinimumShouldMatch").paramName = "minimum_should_match." + common.fields.get("highFreqMinimumShouldMatch").paramName;

        QueryDSLType multi = result.get("multi_match");
        multi.fields.put("type", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.STRING, "type"));
        multi.fields.put("zeroTermsQuery", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.STRING, "zero_terms_query"));

        QueryDSLType match = result.get("match");
        match.fields.put("zeroTermsQuery", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.STRING, "zero_terms_query"));

        QueryDSLType queryString = result.get("query_string");
        queryString.fields.put("locale", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.STRING, "locale"));

        QueryDSLType template = result.get("template");
        if(template != null) {
            template.fields.clear();
            template.fields.put("file", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.STRING, "file"));
            template.fields.put("id", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.STRING, "id"));
            template.fields.put("query", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.QUERY_BUILDER, "query"));
            template.fields.put("params", new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.MAP, "params"));
        }
    }

    public static Map<String, QueryDSLType> parseFilters(String baseDir, ClassLoader classLoader) throws ClassNotFoundException {
        Map<String, QueryDSLType> result = new TreeMap<String, QueryDSLType>();
        File dir = new File(baseDir);
        parse(result, dir, QueryDSLType.TYPE.FILTER, classLoader);
        applyFilterBuilderWorkarounds(result);

        return result;
    }

    protected static void applyFilterBuilderWorkarounds(Map<String, QueryDSLType> result) {
        QueryDSLType geo_distance_range = result.get("geo_distance_range");
        geo_distance_range.setNamedObject(false);
        geo_distance_range.setNamedObjectValue(false);
        geo_distance_range.fields.get("optimizeBbox").type = QueryDSLType.PARAM_TYPE.OPTIMIZE_BBOX;

        QueryDSLType geo_distance = result.get("geo_distance");
        geo_distance.fields.get("optimizeBbox").type = QueryDSLType.PARAM_TYPE.OPTIMIZE_BBOX;
        geo_distance.setNamedObject(false);
        geo_distance.setNamedObjectValue(false);


        QueryDSLType geo_bbox = result.get("geo_bbox");
        geo_bbox.fields.put("top_left[0]",
            new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.DOUBLE, "top_left[0]", "left"));
        geo_bbox.fields.put("top_left[1]",
            new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.DOUBLE, "top_left[1]", "top"));
        geo_bbox.fields.put("bottom_right[0]",
            new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.DOUBLE, "bottom_right[0]", "right"));
        geo_bbox.fields.put("bottom_right[1]",
            new QueryDSLType.Param(QueryDSLType.PARAM_TYPE.DOUBLE, "bottom_right[1]", "bottom"));
    }

    private static void parse(Map<String, QueryDSLType> results, File dir, QueryDSLType.TYPE _type, ClassLoader classLoader) throws ClassNotFoundException {
        final String typePrefix = _type == QueryDSLType.TYPE.QUERY ? "Query" : "Filter";
        for (String f : dir.list(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(typePrefix + "Builder.java");
            }
        })) {
            String name = f.replace(typePrefix + "Builder.java", "");
            if (name.length() == 0 || !new File(dir, name + typePrefix + "Parser.java").exists()) continue;
            Class builderClass = Class.forName("org.elasticsearch.index.query." + name + typePrefix + "Builder", true, classLoader);
            Class parserClass = Class.forName("org.elasticsearch.index.query." + name + typePrefix + "Parser", true, classLoader);
            try {
                // get query
                QueryDSLType dslType = new QueryDSLType((String) parserClass.getField("NAME").get(null), _type);

                // find param names in the builder file
                StringBuilder sb = new StringBuilder();
                BufferedReader br = new BufferedReader(new FileReader(new File(dir, f)));
                String s = null;
                while ((s = br.readLine()) != null) {
                    sb.append(s).append("\n");
                }
                br.close();
                String input = sb.toString();

                //=========================================================
                // Handle namedObject fields. Note: see below
                // (at the end of this code block for handling of shortcut version)
                // it has to be at the end because it removes fields
                //=========================================================
                int idx = input.indexOf("builder.startObject(name);");
                if (idx > -1) {
                    dslType.setNamedObject(true);
                    // querybuilders often have a shortcut version, skip these
                    // e.g. PrefixQueryBuilder
                    // if (boost == -1 && rewrite == null && type != null) {
                    // builder.field(name, prefix);
                    input = input.substring(idx, input.length());
                } else {
                    final String pattern = "builder.startArray(name);";
                    idx = input.indexOf(pattern);
                    if (idx > -1) {
                        dslType.setNamedArray(true);
                        // querybuilders often have a shortcut version, skip these
                        input = input.substring(idx + pattern.length(), input.length());
                    }
                }

                // get fields
//                System.out.println(builderClass);
                for (Field field : builderClass.getDeclaredFields()) {
                    if ((dslType.isNamedArray() || dslType.isNamedObject())
                        && field.getName().equals("name")) continue;
                    field.setAccessible(true);
                    dslType.addField(field);
//                    System.out.println(field.getName());
                }

                //=========================================================
                // Resolves the following patterns:
                // builder.field("param", field)
                // builder.array("param", field)
                //=========================================================
                Pattern p = Pattern.compile("\\.(?:field|array)\\(\"(.*?)\",\\s*(.*?)\\)");
                Matcher m = p.matcher(input);
                while (m.find()) {
                    String fieldName = m.group(2);
                    fieldName = fieldName.replace("this.", "").replaceAll("\\..+", "");
                    String paramName = m.group(1);

                    // workaround for 1.7.2 silliness
                    if(fieldName.equals("!include")) {
//                        fieldName = "exclude";
                        paramName = "include";
                        fieldName = "include";
                    }
                    dslType.setParamName(fieldName, paramName);
                }

                //=========================================================
                // Resolves the following patterns:
                // builder.startArray("param");
                // for (String s: field)
                //=========================================================
                p = Pattern.compile("builder\\.startArray\\(\"(.*?)\"\\);\\s*?for \\(.*?: (.*?)\\)", Pattern.DOTALL);
                m = p.matcher(input);
                while (m.find()) {
                    String fieldName = m.group(2);
                    fieldName = fieldName.replace("this.", "").replaceAll("\\..+", "");
                    String paramName = m.group(1);
                    dslType.setParamName(fieldName, paramName);
                }

                //=========================================================
                // Resolves the following patterns:
                // builder.field("param");
                // field.toXContent(builder, params);
                //=========================================================
                p = Pattern.compile("builder\\.field\\(\"([^\"]*?)\"\\);\\s*?([a-zA-Z]+?)\\.toXContent\\(builder, params\\)", Pattern.DOTALL);
                m = p.matcher(input);
                while (m.find()) {
                    String fieldName = m.group(2);
                    fieldName = fieldName.replace("this.", "").replaceAll("\\..+", "");
                    String paramName = m.group(1);
                    dslType.setParamName(fieldName, paramName);
                }

                //=========================================================
                // Resolves the following patterns:
                // doXArrayContent("param", field, xxx, xxx);
                //=========================================================
                p = Pattern.compile("doXArrayContent\\(\"(.*?)\", (.*?),");
                m = p.matcher(input);
                while (m.find()) {
                    String fieldName = m.group(2);
                    fieldName = fieldName.replace("this.", "").replaceAll("\\..+", "");
                    String paramName = m.group(1);
                    dslType.setParamName(fieldName, paramName);
                }

                //=========================================================
                // Resolves the following patterns:
                // builder.startArray(name).value(lon).value(lat).endArray();
                //=========================================================
                p = Pattern.compile("builder\\.startArray\\(name\\)((?:\\.value\\(([^)]+)\\))+)\\.endArray\\(\\);");
                m = p.matcher(input);
                while (m.find()) {
                    // most regex engines don't allow repeating matched groups.
                    // so we have to run another regex to get them all
                    String tmp = m.group(1);
                    List<String> list = new ArrayList<String>();
                    Pattern p2 = Pattern.compile("value\\((.*?)\\)");
                    Matcher m2 = p2.matcher(tmp);
                    while (m2.find()) {
                        final String field = m2.group(1);
                        list.add(field);
//                        dslType.setParamName(field, field);
                    }
                    String namedArrayValues = StringUtils.join(list, ",");
                    dslType.setNamedArray(true);
                    dslType.setNamedArrayValues(namedArrayValues);

                }

                //=========================================================
                // The following patterns resolve code in the form:
                // builder.field(MoreLikeThisQueryParser.FIELDS.LIKE_THIS.getPreferredName()
                // using resolveParamName()
                //=========================================================
                p = Pattern.compile("\\.(?:field|array)\\(([^\"]*?),\\s+(.*?)\\)", Pattern.DOTALL);
                m = p.matcher(input);
                while (m.find()) {
                    String fieldName = m.group(2);
                    fieldName = fieldName.replace("this.", "").replaceAll("\\..+", "");
                    String paramName = m.group(1);
                    paramName = resolveParamName(paramName, classLoader);
                    dslType.setParamName(fieldName, paramName);
                }

                p = Pattern.compile("\\.startArray\\(([^\"]*?)\\);.*?for \\(.*?: (.*?)\\)", Pattern.DOTALL);
                m = p.matcher(input);
                while (m.find()) {
                    String fieldName = m.group(2);
                    fieldName = fieldName.replace("this.", "").replaceAll("\\..+", "");
                    String paramName = m.group(1);
                    paramName = resolveParamName(paramName, classLoader);
                    dslType.setParamName(fieldName, paramName);
                }

                //=========================================================
                // PrefixFilterBuilder only has the shortcut version of namedObject,
                // so we're forced to handle this here.
                //      builder.field(name, value);
                // This has to be run after all other patterns because it removes
                // fields from dslType.fields.
                //=========================================================
                p = Pattern.compile("builder\\.field\\((name),\\s*(.*?)\\);");
                m = p.matcher(input);
                while (m.find()) {
                    dslType.setNamedObject(true);
                    dslType.setNamedObjectValue(true);
                    String fieldName = m.group(2);
                    String paramName = m.group(1);
                    // remove these from fields. this will get set directly in the interface
                    dslType.fields.remove(fieldName);
                    dslType.fields.remove(paramName);
                }

                if(dslType.isNamedArray())
                    dslType.fields.remove("values");

                if(dslType.isNamedObject())
                    dslType.fields.remove("value");

                results.put(dslType.dslName, dslType);

            } catch (NoSuchFieldException e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static String toJSON(Map<String, QueryDSLType> queries, Map<String, QueryDSLType> filters, boolean outputJavaFields, boolean prettyprint) throws IOException {
        XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent);
        if (prettyprint) builder.prettyPrint();
        builder.startObject();
        builder.startObject("query");
        output(queries, outputJavaFields, builder);
        builder.endObject();
        builder.startObject("filter");
        output(filters, outputJavaFields, builder);
        builder.endObject();
        builder.endObject();
        return builder.string();
    }

    private static void output(Map<String, QueryDSLType> map, boolean outputJavaFields, XContentBuilder builder) throws IOException {
        for (QueryDSLType type : map.values()) {
            builder.startObject(type.dslName);
            if (type.isNamedObject()) {
                builder.field("namedObject", type.namedObject);
                if (type.isNamedObjectValue()) {
                    builder.field("namedObjectValue", type.namedObjectValue);
                }
            } else if (type.isNamedArray()) {
                builder.field("namedArray", type.namedArray);
                if (type.getNamedArrayValues() != null) {
                    builder.field("namedArrayValues", type.namedArrayValues);
                }
            }

            builder.startArray("fields");
            for (String s : type.fields.keySet()) {
                QueryDSLType.Param p = type.fields.get(s);
                if (p.paramName == null || p.type == QueryDSLType.PARAM_TYPE.FILTER_BUILDER
                    || p.type == QueryDSLType.PARAM_TYPE.QUERY_BUILDER || p.type == QueryDSLType.PARAM_TYPE.LIST_QUERY_BUILDER) continue;
                builder.startObject();
                builder.field("name", p.paramName);
                builder.field("type", p.type);
                if(p.label != null) builder.field("label", p.label);
                if (outputJavaFields) builder.field("javaField", s);
                builder.endObject();
            }
            for (String s : type.fields.keySet()) {
                QueryDSLType.Param p = type.fields.get(s);
                if (p.paramName == null || (p.type != QueryDSLType.PARAM_TYPE.FILTER_BUILDER
                    && p.type != QueryDSLType.PARAM_TYPE.QUERY_BUILDER
                    && p.type != QueryDSLType.PARAM_TYPE.LIST_QUERY_BUILDER))
                    continue;
                builder.startObject();
                builder.field("name", p.paramName);
                builder.field("type", p.type);
                if (outputJavaFields) builder.field("javaField", s);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
    }

    /**
     * Some params instead of being set by builder.field("param", field)
     * are set by static constants, e.g. builder.field(FooQueryParser.Fields.BLAH.getPreferredName()
     * This method attempts to resolve those parameter names.
     */
    private static String resolveParamName(String paramName, ClassLoader classLoader) {
        Pattern p = Pattern.compile("^(.*?)\\.Fields\\.(.*?)\\.getPreferredName\\(\\)");
        Matcher m = p.matcher(paramName);
        if (m.find()) {
            try {
                Class clazz = Class.forName("org.elasticsearch.index.query." + m.group(1), true, classLoader);
                for (Class c : clazz.getClasses()) {
                    if (c.getSimpleName().endsWith("Fields")) {
                        clazz = c;
                        break;
                    }
                }
                Field f = clazz.getField(m.group(2));
                Object o = f.get(null);
                return (String) o.getClass().getMethod("getPreferredName").invoke(o);
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        return paramName;
    }
}
