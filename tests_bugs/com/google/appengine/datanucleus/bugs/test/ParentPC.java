package com.google.appengine.datanucleus.bugs.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class ParentPC {
  @SuppressWarnings("unused")
  @PrimaryKey 
  @Persistent(valueStrategy=IdGeneratorStrategy.IDENTITY)
  private Key primaryKey;
  
  @Persistent(embedded="true")
  private ChildPC child;

  public void setChild(ChildPC child) {
    this.child = child;
  }

  public ChildPC getChild() {
    return child;
  }
}