// Copyright 2008 Google Inc. All Rights Reserved.
package org.datanucleus.test;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class BidirectionalChildListStringPkJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @ManyToOne
  private HasOneToManyListStringPkJPA parent;

  private String childVal;

  public HasOneToManyListStringPkJPA getParent() {
    return parent;
  }

  public void setParent(HasOneToManyListStringPkJPA parent) {
    this.parent = parent;
  }

  public String getId() {
    return id;
  }

  public String getChildVal() {
    return childVal;
  }

  public void setChildVal(String childVal) {
    this.childVal = childVal;
  }

  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BidirectionalChildListStringPkJPA that = (BidirectionalChildListStringPkJPA) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    return (id != null ? id.hashCode() : 0);
  }
}