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
public class HasAncestorJDO {
  @Persistent
  @Extension(vendorName="datanucleus", key="ancestor-pk", value="true")
  private String ancestorId;

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  public HasAncestorJDO() {
  }

  public HasAncestorJDO(String ancestorId) {
    this(ancestorId, null);
  }

  public HasAncestorJDO(String ancestorId, String id) {
    this.ancestorId = ancestorId;
    this.id = id;
  }

  public String getAncestorId() {
    return ancestorId;
  }

  public String getId() {
    return id;
  }
}
