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
public class BidirectionalChildArrayJDO implements BidirectionalChildJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  private String childVal;

  @Persistent
  private HasOneToManyArrayJDO parent;

  public String getId() {
    return id;
  }

  public HasOneToManyArrayJDO getParent() {
    return parent;
  }

  public void setParent(HasOneToManyJDO parent) {
    this.parent = (HasOneToManyArrayJDO) parent;
  }

  public String getChildVal() {
    return childVal;
  }

  public void setChildVal(String childVal) {
    this.childVal = childVal;
  }

}