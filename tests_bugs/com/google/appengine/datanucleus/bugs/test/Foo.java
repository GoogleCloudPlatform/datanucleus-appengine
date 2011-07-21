package com.google.appengine.datanucleus.bugs.test;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;

import com.google.appengine.api.datastore.Key;

@Entity
public class Foo {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  @SuppressWarnings("unused")
  @Basic
  @Column(name="barKey")
  private Key barKey;

  @SuppressWarnings("unused")
  @OneToOne(fetch = FetchType.LAZY)
  @Column(name = "barKey", insertable = false, updatable = false)
  private Bar barAlias;

  public Long getId() {
    return id;
  }
}
