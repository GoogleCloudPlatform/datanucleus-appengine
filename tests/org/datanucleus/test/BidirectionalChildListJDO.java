// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class BidirectionalChildListJDO implements BidirectionalChildJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
  private String id;

  @Persistent
  private HasOneToManyListJDO parent;

  @Persistent
  private String childVal;

  public String getId() {
    return id;
  }

  public HasOneToManyListJDO getParent() {
    return parent;
  }

  public void setParent(HasOneToManyJDO parent) {
    this.parent = (HasOneToManyListJDO) parent;
  }

  public String getChildVal() {
    return childVal;
  }

  public void setChildVal(String childVal) {
    this.childVal = childVal;
  }
}
