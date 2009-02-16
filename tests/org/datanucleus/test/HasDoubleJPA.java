// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Entity;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasDoubleJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  private double aDouble;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public double getADouble() {
    return aDouble;
  }

  public void setADouble(double aDouble) {
    this.aDouble = aDouble;
  }
}
