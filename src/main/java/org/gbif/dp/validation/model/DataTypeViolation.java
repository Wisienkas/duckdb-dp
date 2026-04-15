package org.gbif.dp.validation.model;

import java.util.List;

/**
 * Reports rows in a resource where a column value does not match the declared Frictionless type.
 *
 * @param resource      name of the resource containing the violation
 * @param field         column name
 * @param declaredType  the Frictionless type declared in the schema (e.g. "integer", "date")
 * @param violationCount total number of non-null values that fail the cast
 * @param sampleValues  a small list of actual values that could not be cast
 */
public record DataTypeViolation(
    String resource,
    String field,
    String declaredType,
    long violationCount,
    List<String> sampleValues) {}

