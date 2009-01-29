// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasOneToOneParentKeyPkJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key key;

  @Persistent(mappedBy = "hasParentKeyPK")
  private HasOneToOneJDO parent;

  private String str;

  public Key getKey() {
    return key;
  }

  public HasOneToOneJDO getParent() {
    return parent;
  }

  public void setParent(HasOneToOneJDO parent) {
    this.parent = parent;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }
}