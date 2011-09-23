package com.google.appengine.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class Issue62Parent {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;
 
  @Persistent
  private Issue62Child child; 
  
  public Issue62Parent(Issue62Child child) {
    this.child = child;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Issue62Child getChild() {
    return child;
  }

  public void setChild(Issue62Child child) {
    this.child = child;
  }
}
