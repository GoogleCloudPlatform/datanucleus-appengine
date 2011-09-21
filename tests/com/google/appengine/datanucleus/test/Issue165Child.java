package com.google.appengine.datanucleus.test;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class Issue165Child {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value = "true")
  private String id;

  @Persistent(mappedBy = "child")
  Issue165Parent parent;
  
  @Persistent
  String aString;

  public String getId() {
    return id;
  }

  public void setAString(String str) {
    aString = str;
  }

  public void setParent(Issue165Parent parent) {
    this.parent = parent;
  }

  public Issue165Parent getParent() {
    return parent;
  }
}
