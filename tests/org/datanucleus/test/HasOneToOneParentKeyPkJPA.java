// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToOneParentKeyPkJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  private Key id;

  @OneToOne(mappedBy = "hasParentKeyPK")
  private HasOneToOneJPA parent;

  private String str;

  public Key getId() {
    return id;
  }

  public HasOneToOneJPA getParent() {
    return parent;
  }

  public void setParent(HasOneToOneJPA parent) {
    this.parent = parent;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }
}
