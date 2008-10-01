// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import com.google.apphosting.api.datastore.Key;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasStringAncestorKeyPkJPA {

  // I think according to the spec this isn't valid - you can't
  // have fields of custom types that aren't part of the PK.
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private Key key;

  @Extension(vendorName="datanucleus", key="ancestor-pk", value="true")
  private String ancestorKey;

  public Key getKey() {
    return key;
  }

  public void setKey(Key key) {
    this.key = key;
  }

  public String getAncestorKey() {
    return ancestorKey;
  }

  public void setAncestorKey(String ancestorKey) {
    this.ancestorKey = ancestorKey;
  }
}
