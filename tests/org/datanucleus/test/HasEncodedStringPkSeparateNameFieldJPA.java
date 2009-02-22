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
public class HasEncodedStringPkSeparateNameFieldJPA {

  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @Extension(vendorName="datanucleus", key="gae.pk-name", value="true")
  private String name;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}