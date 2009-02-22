// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasAncestorJPA {

  @Extension(vendorName="datanucleus", key="gae.parent-pk", value="true")
  private String ancestorId;

  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  public HasAncestorJPA() {
  }

  public HasAncestorJPA(String ancestorId) {
    this(ancestorId, null);
  }

  public HasAncestorJPA(String ancestorId, String id) {
    this.ancestorId = ancestorId;
    this.id = id;
  }

  public String getAncestorId() {
    return ancestorId;
  }

  public String getId() {
    return id;
  }
}
