package com.google.appengine.datanucleus.query;

/**
 * Simple query result class.
 */
public class FlightStartEnd2 {
  String origin;
  String dest;

  public FlightStartEnd2(String origin, String dest) {
    this.origin = origin;
    this.dest = dest;
  }

  public String getOrigin() {
    return origin;
  }

  public String getDest() {
    return dest;
  }
}
