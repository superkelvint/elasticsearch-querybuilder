package com.adamantite.es.querybuilder;

import static com.adamantite.es.querybuilder.QueryDSLTypeExtractor.esQuerybuilderDir;
import static com.adamantite.es.querybuilder.QueryDSLTypeExtractor.esVersion;
import static com.adamantite.es.querybuilder.QueryDSLTypeExtractor.classLoader;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

public class QueryDSLTypeExtractorTest {

    @BeforeClass
    public void init() throws MalformedURLException {
        QueryDSLTypeExtractor.init();
    }


    @Test
    public void testParseFilters() throws ClassNotFoundException {
        Map<String, QueryDSLType> parsed = QueryDSLTypeExtractor.parseFilters(esQuerybuilderDir, classLoader);

        QueryDSLType ids = parsed.get("ids");
        checkFields(new String[]{"types", "values", "_name"},
            ids.fields);

        QueryDSLType has_parent = parsed.get("has_parent");
//        checkFields(new String[]{"parent_type", "_name", "query", "filter"},
//            has_parent.fields);
        assertEquals(QueryDSLType.PARAM_TYPE.QUERY_BUILDER, has_parent.fields.get("queryBuilder").type);
        assertEquals(QueryDSLType.PARAM_TYPE.FILTER_BUILDER, has_parent.fields.get("filterBuilder").type);

        QueryDSLType bool = parsed.get("bool");
        checkFields(new String[]{"must", "must_not", "should", "_cache_key", "_cache", "_name"},
            bool.fields);
        assertEquals(QueryDSLType.PARAM_TYPE.LIST_FILTER_BUILDER, bool.fields.get("mustClauses").type);
        assertEquals(QueryDSLType.PARAM_TYPE.LIST_FILTER_BUILDER, bool.fields.get("shouldClauses").type);
        assertEquals(QueryDSLType.PARAM_TYPE.LIST_FILTER_BUILDER, bool.fields.get("mustNotClauses").type);

        QueryDSLType prefix = parsed.get("prefix");
        checkFields(new String[]{"_cache_key", "_cache", "_name"},
            prefix.fields);
        assertTrue(prefix.namedObject);
        assertTrue(prefix.namedObjectValue);

        QueryDSLType terms = parsed.get("terms");
        checkFields(new String[]{"_cache_key", "_cache", "_name", "execution"},
            terms.fields);
        assertTrue(terms.namedObject);
        assertTrue(terms.namedObjectValue);

        QueryDSLType geofilt = parsed.get("geo_distance");
        assertTrue(geofilt.namedArray);
        assertEquals("lon,lat", geofilt.namedArrayValues);
    }

    @Test
    public void testParseQueries() throws ClassNotFoundException {
        Map<String, QueryDSLType> parsed = QueryDSLTypeExtractor.parseQueries(esQuerybuilderDir, classLoader);

        QueryDSLType ids = parsed.get("ids");
        checkFields(new String[]{"types", "values", "boost", "_name"},
            ids.fields);


//        QueryDSLType has_parent = parsed.get("has_parent");
//        checkFields(new String[]{"parent_type", "score_type", "boost", "_name", "query"},
//            has_parent.fields);

        QueryDSLType boosting = parsed.get("boosting");
        checkFields(new String[]{"negative_boost", "boost", "negative", "positive"},
            boosting.fields);
        assertEquals(QueryDSLType.PARAM_TYPE.QUERY_BUILDER, boosting.fields.get("positiveQuery").type);

        QueryDSLType indices = parsed.get("indices");
        checkFields(new String[]{"no_match_query", "indices", "query", "_name", "indices"},
            indices.fields);
        assertEquals(QueryDSLType.PARAM_TYPE.QUERY_BUILDER, indices.fields.get("queryBuilder").type);
        assertEquals(QueryDSLType.PARAM_TYPE.QUERY_BUILDER, indices.fields.get("noMatchQuery").type);

        QueryDSLType bool = parsed.get("bool");
        checkFields(new String[]{"must", "must_not", "should", "boost", "disable_coord", "minimum_should_match", "adjust_pure_negative", "_name"},
            bool.fields);
        assertEquals(QueryDSLType.PARAM_TYPE.LIST_QUERY_BUILDER, bool.fields.get("mustClauses").type);
        assertEquals(QueryDSLType.PARAM_TYPE.LIST_QUERY_BUILDER, bool.fields.get("shouldClauses").type);
        assertEquals(QueryDSLType.PARAM_TYPE.LIST_QUERY_BUILDER, bool.fields.get("mustNotClauses").type);

        QueryDSLType flt = parsed.get("flt");
        checkFields(new String[]{"fields", "like_text", "max_query_terms", "fuzziness",
            "prefix_length", "ignore_tf", "boost", "analyzer", "fail_on_unsupported_field", "_name"},
            flt.fields);

        QueryDSLType mlt = parsed.get("mlt");
        checkFields(new String[]{"fields", "like_text", "max_query_terms", "minimum_should_match",
            "analyzer", "include", "max_word_length", "min_term_freq", "stop_words",
            "min_doc_freq", "max_doc_freq", "min_word_length", "boost_terms",
            "ids", "docs", "fail_on_unsupported_field", "_name", "boost"},
            mlt.fields);

        QueryDSLType multi_match = parsed.get("multi_match");
        checkFields(new String[]{"query", "fields", "type", "operator", "analyzer",
            "boost", "slop", "prefix_length", "max_expansions",
            "minimum_should_match", "rewrite", "fuzzy_rewrite",
            "use_dis_max", "tie_breaker", "lenient", "cutoff_frequency",
            "zero_terms_query", "_name", "fuzziness"},
            multi_match.fields);

        QueryDSLType common = parsed.get("common");
        assertEquals("minimum_should_match.low_freq", common.fields.get("lowFreqMinimumShouldMatch").paramName);
    }

    @Test
    public void testNamedObjectFields() throws ClassNotFoundException {
        Map<String, QueryDSLType> parsed = QueryDSLTypeExtractor.parseQueries(esQuerybuilderDir, classLoader);

        QueryDSLType prefix = parsed.get("prefix");
        assertEquals("prefix", prefix.fields.get("prefix").paramName);
        assertTrue(prefix.namedObject);
    }

    private void checkFields(String[] strings, Map<String, QueryDSLType.Param> set) {
        assertEquals(printListDifference(strings, set), strings.length, set.size());
        for (String s : strings) {
            boolean found = false;
            for (QueryDSLType.Param p : set.values()) {
                if (s.equals(p.paramName)) {
                    found = true;
                    break;
                }
            }
            assertTrue(printParams(set) + " doesn't contain " + s, found);
        }
    }

    private String printListDifference(String[] strings, Map<String, QueryDSLType.Param> set) {
        StringBuilder sb = new StringBuilder();
        for(String s: strings) {
            sb.append(s + "\n");
        }
        sb.append("==================\n\n");
        for(String s: set.keySet()) {
            sb.append(s + "\n");
        }
        return sb.toString();
    }

    private String printParams(Map<String, QueryDSLType.Param> set) {
        StringBuilder sb = new StringBuilder();
        for (QueryDSLType.Param p : set.values()) {
            sb.append(p.paramName).append(",");
        }
        return sb.toString();
    }

    @Test
    public void testToJSON() throws IOException, ClassNotFoundException {
        Map<String, QueryDSLType> queries = QueryDSLTypeExtractor.parseQueries(esQuerybuilderDir, classLoader);
        Map<String, QueryDSLType> filters = QueryDSLTypeExtractor.parseFilters(esQuerybuilderDir, classLoader);
        String json = QueryDSLTypeExtractor.toJSON(queries, filters, false, true);
        BufferedWriter writer = new BufferedWriter(new FileWriter("/tmp/qb-model-"+ esVersion +".json"));
        writer.write(json);
        writer.close();
    }

}
