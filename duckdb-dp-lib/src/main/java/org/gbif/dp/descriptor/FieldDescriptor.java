package org.gbif.dp.descriptor;

import java.util.List;

/**
 * Describes a single field in a Frictionless Data Package resource schema.
 *
 * @param name   the column name
 * @param type   the Frictionless type (string, integer, number, boolean, date, datetime, time, year, object, array, etc.)
 * @param format optional format hint (e.g. "default", "email", "uri", a date pattern, etc.)
 */
public record FieldDescriptor(String name, String type, String format, List<MissingValueDescriptor> missingValues) {

  /** Convenience constructor when no format is specified. */
  public FieldDescriptor(String name, String type, List<MissingValueDescriptor> missingValues) {
    this(name, type, "default", missingValues);
  }
}

