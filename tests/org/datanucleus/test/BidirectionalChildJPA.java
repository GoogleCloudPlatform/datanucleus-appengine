// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class BidirectionalChildJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private String id;

  @ManyToOne
  private HasOneToManyJPA parent;

  private String childVal;

  public HasOneToManyJPA getParent() {
    return parent;
  }

  public void setParent(HasOneToManyJPA parent) {
    this.parent = parent;
  }

  public String getId() {
    return id;
  }

  public String getChildVal() {
    return childVal;
  }

  public void setChildVal(String childVal) {
    this.childVal = childVal;
  }
}
