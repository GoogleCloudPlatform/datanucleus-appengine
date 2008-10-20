// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.Embedded;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class Person {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  @Embedded
  private Name name;

  @Persistent
  @Embedded(members = {
    @Persistent(name = "first", columns=@Column(name="anotherFirst")),
    @Persistent(name = "last", columns=@Column(name="anotherLast"))}
  )
  private Name anotherName;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Name getName() {
    return name;
  }

  public void setName(Name name) {
    this.name = name;
  }

  public Name getAnotherName() {
    return anotherName;
  }

  public void setAnotherName(Name anotherName) {
    this.anotherName = anotherName;
  }
}
