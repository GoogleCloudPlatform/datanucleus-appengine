/**********************************************************************
Copyright (c) 2011 Google Inc.

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
package com.google.appengine.datanucleus.test;

import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirBottom;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirBottomLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirBottomUnencodedStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirMiddle;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirMiddleLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirMiddleUnencodedStringPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTop;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopLongPk;
import com.google.appengine.datanucleus.test.BidirectionalSingleTableChildJPA.BidirTopUnencodedStringPk;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyLongPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsJPA.HasOneToManyUnencodedStringPkJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManyLongPkSetJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManySetJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsSetJPA.HasOneToManyUnencodedStringPkSetJPA;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

import static com.google.appengine.datanucleus.PolymorphicTestUtils.getEntityKind;

public class BidirectionalSingleTableChildSetJPA {
  
  @Entity
  @DiscriminatorColumn(name = "DTYPE")
  public static class BidirTopSet implements BidirTop {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasOneToManySetJPA parent;

    private String childVal;

    public BidirTopSet() {
    }

    public BidirTopSet(String id) {
      this.id = KeyFactory.createKeyString(getEntityKind(BidirTopSet.class), id);
    }

    public HasOneToManySetJPA getParent() {
      return parent;
    }

    public void setParent(HasOneToManyJPA parent) {
      this.parent = (HasOneToManySetJPA) parent;
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

      BidirTopSet that = (BidirTopSet) o;

      if (id != null ? !id.equals(that.id) : that.id != null) {
        return false;
      }

      return true;
    }

    public int hashCode() {
      return (id != null ? id.hashCode() : 0);
    }

    public int getPropertyCount() {
      return 2;
    }

  }
  
  @Entity
  public static class BidirMiddleSet extends BidirTopSet implements BidirMiddle {

    public BidirMiddleSet() {
    }
    
    public BidirMiddleSet(String id) {
      super(id);
    }
    
    private Long middleChildVal;

    public Long getMiddleChildVal() {
      return middleChildVal;
    }

    public void setMiddleChildVal(Long middleChildVal) {
      this.middleChildVal = middleChildVal;
    }

    public int getPropertyCount() {
      return 3;
    }
  }
  
  @Entity
  public static class BidirBottomSet extends BidirMiddleSet implements BidirBottom {
    
    public BidirBottomSet() {
    }
    
    public BidirBottomSet(String id) {
      super(id);
    }
    
    private Double bottomChildVal;

    public Double getBottomChildVal() {
      return bottomChildVal;
    }

    public void setBottomChildVal(Double bottomChildVal) {
      this.bottomChildVal = bottomChildVal;
    }
  
    public int getPropertyCount() {
      return 4;
    }
  }
  
  @Entity
  @DiscriminatorValue(value = "T")
  public static class BidirTopLongPkSet implements BidirTopLongPk {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasOneToManyLongPkSetJPA parent;

    private String childVal;

    public HasOneToManyLongPkJPA getParent() {
      return parent;
    }

    public void setParent(HasOneToManyLongPkJPA parent) {
      this.parent = (HasOneToManyLongPkSetJPA) parent;
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

      BidirTopLongPkSet that = (BidirTopLongPkSet) o;

      if (id != null ? !id.equals(that.id) : that.id != null) {
        return false;
      }

      return true;
    }

    public int hashCode() {
      return (id != null ? id.hashCode() : 0);
    }
    
  }
  
  @Entity
  @DiscriminatorValue(value = "M")
  public static class BidirMiddleLongPkSet extends BidirTopLongPkSet implements BidirMiddleLongPk {
  }
  
  @Entity
  @DiscriminatorValue(value = "B")
  public static class BidirBottomLongPkSet extends BidirMiddleLongPkSet implements BidirBottomLongPk {
  }

  @Entity
  @DiscriminatorColumn(name = "TYPE", discriminatorType = DiscriminatorType.CHAR)
  @DiscriminatorValue(value = "A")
  public static class BidirTopUnencodedStringPkSet implements BidirTopUnencodedStringPk {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasOneToManyUnencodedStringPkSetJPA parent;

    private String childVal;

    public HasOneToManyUnencodedStringPkSetJPA getParent() {
      return parent;
    }

    public void setParent(HasOneToManyUnencodedStringPkJPA parent) {
      this.parent = (HasOneToManyUnencodedStringPkSetJPA) parent;
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

      BidirTopUnencodedStringPkSet that = (BidirTopUnencodedStringPkSet) o;

      if (id != null ? !id.equals(that.id) : that.id != null) {
        return false;
      }

      return true;
    }

    public int hashCode() {
      return (id != null ? id.hashCode() : 0);
    }
    
    public int getPropertyCount() {
      return 2;
    }
  }
  
  @Entity
  @DiscriminatorValue(value = "B")
  public static class BidirMiddleUnencodedStringPkSet extends BidirTopUnencodedStringPkSet implements BidirMiddleUnencodedStringPk {
    private String middleChildVal;
    
    public String getMiddleChildVal() {
      return middleChildVal;
    }

    public void setMiddleChildVal(String middleChildVal) {
      this.middleChildVal = middleChildVal;
    }

    public int getPropertyCount() {
      return 3;
    }
  }
  
  @Entity
  @DiscriminatorValue(value = "C")
  public static class BidirBottomUnencodedStringPkSet extends BidirMiddleUnencodedStringPkSet implements BidirBottomUnencodedStringPk {
    private String bottomChildVal;
    
    public String getBottomChildVal() {
      return bottomChildVal;
    }

    public void setBottomChildVal(String bottomChildVal) {
      this.bottomChildVal = bottomChildVal;
    }
    
    public int getPropertyCount() {
      return 4;
    }
  }
}
