// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

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
public class HasKeyPkJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Key key;

  @Persistent
  @Extension(vendorName="datanucleus", key="ancestor-pk", value="true")
  private Key ancestorKey;

  @Persistent
  private String str;

  public Key getKey() {
    return key;
  }

  public void setKey(Key key) {
    this.key = key;
  }

  public Key getAncestorKey() {
    return ancestorKey;
  }

  public void setAncestorKey(Key ancestorKey) {
    this.ancestorKey = ancestorKey;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }
}
