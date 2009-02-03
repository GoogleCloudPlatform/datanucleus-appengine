// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class BidirectionalChildSetJDO implements BidirectionalChildJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  private HasOneToManySetJDO parent;

  @Persistent
  private String childVal;

  public String getId() {
    return id;
  }

  public HasOneToManySetJDO getParent() {
    return parent;
  }

  public void setParent(HasOneToManyJDO parent) {
    this.parent = (HasOneToManySetJDO) parent;
  }

  public String getChildVal() {
    return childVal;
  }

  public void setChildVal(String childVal) {
    this.childVal = childVal;
  }
}