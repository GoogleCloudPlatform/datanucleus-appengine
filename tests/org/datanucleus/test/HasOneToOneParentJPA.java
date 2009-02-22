// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class HasOneToOneParentJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @OneToOne(mappedBy = "hasParent")
  private HasOneToOneJPA parent;

  private String str;

  public String getId() {
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

  public void setId(String id) {
    this.id = id;
  }
}
