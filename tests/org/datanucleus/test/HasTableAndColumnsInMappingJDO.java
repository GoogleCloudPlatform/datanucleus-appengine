// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;
import javax.jdo.annotations.Version;
import javax.jdo.annotations.VersionStrategy;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION,
    table = HasTableAndColumnsInMappingJDO.TABLE_NAME)
@Version(strategy = VersionStrategy.VERSION_NUMBER)
public class HasTableAndColumnsInMappingJDO {

  public static final String TABLE_NAME = "TABLE_NAME";
  public static final String FOO_COLUMN_NAME = "bar";

  @PrimaryKey(column = "pk")
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent(column = FOO_COLUMN_NAME)
  private String foo;

  public Long getId() {
    return id;
  }

  public String getFoo() {
    return foo;
  }

  public void setFoo(String foo) {
    this.foo = foo;
  }
}
