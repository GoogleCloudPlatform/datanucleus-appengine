// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManagerFactory;

/**
 * @author Max Ross <maxr@google.com>
 */
public class PMF {

  public static final PersistenceManagerFactory pmf;
  static {
    pmf = JDOHelper.getPersistenceManagerFactory("datanucleus.properties");
  }

  private PMF() {}
}
