package org.gbif.dp.descriptor;

/**
 * Describes a single field in a Frictionless Data Package resource schema.
 *
 * @param name   the column name
 * @param type   the Frictionless type (string, integer, number, boolean, date, datetime, time, year, object, array, etc.)
 * @param format optional format hint (e.g. "default", "email", "uri", a date pattern, etc.)
 */
public record FieldDescriptor(String name, String type, String format) {

  /** Convenience constructor when no format is specified. */
  public FieldDescriptor(String name, String type) {
    this(name, type, "default");
  }
}

