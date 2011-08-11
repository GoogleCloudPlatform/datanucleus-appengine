package com.google.appengine.datanucleus.query;

/**
 * Simple query result class.
 */
public class FlightStartEnd1 {
  String origin;
  String dest;

  public FlightStartEnd1() {
  }

  public void setOrigin(String origin) {
    this.origin = origin;
  }

  public void setDest(String dest) {
    this.dest = dest;
  }

  public String getOrigin() {
    return origin;
  }

  public String getDest() {
    return dest;
  }
}
