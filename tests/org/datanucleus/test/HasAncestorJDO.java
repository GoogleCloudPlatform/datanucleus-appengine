// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.Extension;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasAncestorJDO {
  @Persistent
  @Extension(vendorName="datanucleus", key="ancestor-pk", value="true")
  private String ancestorId;

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  public HasAncestorJDO(String ancestorId) {
    this.ancestorId = ancestorId;
  }

  public String getAncestorId() {
    return ancestorId;
  }

  public String getId() {
    return id;
  }
}
