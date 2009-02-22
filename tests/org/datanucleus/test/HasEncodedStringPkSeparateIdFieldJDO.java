// Copyright 2009 Google Inc. All Rights Reserved.
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
public class HasEncodedStringPkSeparateIdFieldJDO {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key="gae.encoded-pk", value="true")
  private String key;

  @Persistent
  @Extension(vendorName = "datanucleus", key="gae.pk-id", value="true")
  private Long id;

  public String getKey() {
    return key;
  }

  public void setId(String key) {
    this.key = key;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }
}