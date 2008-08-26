// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasAncestorJPA {

  @Extension(vendorName="datanucleus", key="ancestor-pk", value="true")
  private String ancestorId;

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;

  public HasAncestorJPA() {
    
  }

  public HasAncestorJPA(String ancestorId) {
    this.ancestorId = ancestorId;
  }

  public String getAncestorId() {
    return ancestorId;
  }

  public String getId() {
    return id;
  }
}
