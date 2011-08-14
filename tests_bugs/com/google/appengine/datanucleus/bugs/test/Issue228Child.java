package com.google.appengine.datanucleus.bugs.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

import com.google.appengine.api.datastore.Key;

@PersistenceCapable
public class Issue228Child {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  Key id;

  @Persistent
  Issue228Parent parent;
  
  @Persistent
  String aString;

  public void setAString(String str) { aString = str;}

  public Issue228Parent getParent() {return parent;}
}
