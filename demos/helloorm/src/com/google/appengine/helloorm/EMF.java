// Copyright 2008 Google Inc. All Rights Reserved.
package com.google.appengine.helloorm;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

/**
 * @author Max Ross <maxr@google.com>
 */
public class EMF {
  public static final EntityManagerFactory emf;
  static {
    emf = Persistence.createEntityManagerFactory("helloorm");
  }

  private EMF() {}
}
