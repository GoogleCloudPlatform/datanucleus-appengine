/**********************************************************************
Copyright (c) 2009 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**********************************************************************/
package com.google.appengine.datanucleus.test.jpa;

import org.datanucleus.api.jpa.annotations.Extension;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
@Entity
public class BidirectionalChildUnencodedStringPkListJPA implements BidirectionalChildUnencodedStringPkJPA {
  @Id
  @GeneratedValue(strategy=GenerationType.IDENTITY)
  @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
  private String id;

  @ManyToOne(fetch = FetchType.LAZY)
  private HasOneToManyUnencodedStringPkListJPA parent;

  private String childVal;

  public HasOneToManyUnencodedStringPkListJPA getParent() {
    return parent;
  }

  public void setParent(HasOneToManyUnencodedStringPkJPA parent) {
    this.parent = (HasOneToManyUnencodedStringPkListJPA) parent;
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

    BidirectionalChildUnencodedStringPkListJPA that = (BidirectionalChildUnencodedStringPkListJPA) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }

    return true;
  }

  public int hashCode() {
    return (id != null ? id.hashCode() : 0);
  }
}