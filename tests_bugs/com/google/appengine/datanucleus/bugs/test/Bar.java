package com.google.appengine.datanucleus.bugs.test;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

import com.google.appengine.api.datastore.Key;

@Entity
public class Bar {
  @SuppressWarnings("unused")
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Key key;

}
