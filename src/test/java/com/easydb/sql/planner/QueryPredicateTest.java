package com.easydb.sql.planner;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class QueryPredicateTest {
    @Test
    void testSimplePredicates() {
        QueryPredicate equals = QueryPredicate.equals("age", 25);
        assertEquals("age = 25", equals.toString());

        QueryPredicate notEquals = QueryPredicate.notEquals("name", "John");
        assertEquals("name != John", notEquals.toString());

        QueryPredicate lessThan = QueryPredicate.lessThan("price", 100.0);
        assertEquals("price < 100.0", lessThan.toString());

        QueryPredicate greaterThan = QueryPredicate.greaterThan("quantity", 10);
        assertEquals("quantity > 10", greaterThan.toString());

        QueryPredicate isNull = QueryPredicate.isNull("description");
        assertEquals("description IS NULL", isNull.toString());
    }

    @Test
    void testCompoundPredicates() {
        QueryPredicate age = QueryPredicate.greaterThanEquals("age", 18);
        QueryPredicate city = QueryPredicate.equals("city", "New York");
        
        QueryPredicate and = QueryPredicate.and(Arrays.asList(age, city));
        assertEquals("age >= 18 AND city = New York", and.toString());

        QueryPredicate status = QueryPredicate.equals("status", "active");
        QueryPredicate or = QueryPredicate.or(Arrays.asList(and, status));
        assertEquals("age >= 18 AND city = New York OR status = active", or.toString());
    }

    @Test
    void testNotPredicate() {
        QueryPredicate status = QueryPredicate.equals("status", "inactive");
        QueryPredicate not = QueryPredicate.not(status);
        assertEquals("NOT (status = inactive)", not.toString());
    }

    @Test
    void testPredicateGetters() {
        QueryPredicate pred = QueryPredicate.equals("age", 25);
        
        assertEquals(QueryPredicate.PredicateType.EQUALS, pred.getType());
        assertEquals("age", pred.getColumns().get(0));
        assertEquals(25, pred.getValue());
        assertTrue(pred.getSubPredicates().isEmpty());
    }

    @Test
    void testCompoundPredicateGetters() {
        QueryPredicate age = QueryPredicate.greaterThan("age", 18);
        QueryPredicate city = QueryPredicate.equals("city", "New York");
        QueryPredicate and = QueryPredicate.and(Arrays.asList(age, city));
        
        assertEquals(QueryPredicate.PredicateType.AND, and.getType());
        assertNull(and.getColumn());
        assertNull(and.getValue());
        assertEquals(2, and.getSubPredicates().size());
        assertEquals(age.toString(), and.getSubPredicates().get(0).toString());
        assertEquals(city.toString(), and.getSubPredicates().get(1).toString());
    }

    @Test
    void testEmptyCompoundPredicates() {
        QueryPredicate and = QueryPredicate.and(Arrays.asList());
        assertEquals("", and.toString());

        QueryPredicate or = QueryPredicate.or(Arrays.asList());
        assertEquals("", or.toString());
    }

    @Test
    void testNestedCompoundPredicates() {
        QueryPredicate age = QueryPredicate.greaterThan("age", 18);
        QueryPredicate city = QueryPredicate.equals("city", "New York");
        QueryPredicate and1 = QueryPredicate.and(Arrays.asList(age, city));

        QueryPredicate status = QueryPredicate.equals("status", "active");
        QueryPredicate type = QueryPredicate.equals("type", "user");
        QueryPredicate and2 = QueryPredicate.and(Arrays.asList(status, type));

        QueryPredicate or = QueryPredicate.or(Arrays.asList(and1, and2));
        assertEquals(
            "age > 18 AND city = New York OR status = active AND type = user",
            or.toString()
        );
    }
} 