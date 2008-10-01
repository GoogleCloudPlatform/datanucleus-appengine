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
public class HasKeyAncestorKeyStringPkJPA {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String key;

  // This doesn't actually work - JPA doesn't support non-pk fields
  // of arbitrary types.
  @Extension(vendorName="datanucleus", key="ancestor-pk", value="true")
  private Key ancestorKey;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Key getAncestorKey() {
    return ancestorKey;
  }

  public void setAncestorKey(Key ancestorKey) {
    this.ancestorKey = ancestorKey;
  }
}
