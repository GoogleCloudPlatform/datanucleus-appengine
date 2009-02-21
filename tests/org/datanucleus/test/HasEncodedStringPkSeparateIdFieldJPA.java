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
public class HasEncodedStringPkSeparateIdFieldJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="encoded-pk", value="true")
  private String key;

  @Extension(vendorName = "datanucleus", key="pk-id", value="true")
  private Long id;

  public String getKey() {
    return key;
  }

  public void setId(String key) {
    this.key = key;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }
}