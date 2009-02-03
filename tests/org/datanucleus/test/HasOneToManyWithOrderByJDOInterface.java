// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import java.util.List;

/**
 * @author Max Ross <maxr@google.com>
 */
public interface HasOneToManyWithOrderByJDOInterface {

  List<Flight> getFlightsByOrigAndDest();

  List<Flight> getFlightsByIdAndOrig();

  List<Flight> getFlightsByOrigAndId();
}
