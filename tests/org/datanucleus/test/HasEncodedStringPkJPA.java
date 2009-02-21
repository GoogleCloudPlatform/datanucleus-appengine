// Copyright 2009 Google Inc. All Rights Reserved.
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
public class HasEncodedStringPkJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="encoded-pk", value="true")
  private String id;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}