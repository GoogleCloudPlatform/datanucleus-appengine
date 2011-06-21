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

import org.datanucleus.jpa.annotations.Extension;

import com.google.appengine.datanucleus.Utils;
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
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJPA.HasOneToManyListJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJPA.HasOneToManyLongPkListJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJPA.HasOneToManyStringPkListJPA;
import com.google.appengine.datanucleus.test.HasPolymorphicRelationsListJPA.HasOneToManyUnencodedStringPkListJPA;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;

public class BidirectionalSingleTableChildListJPA {
  @Entity
  @DiscriminatorColumn(name = "DTYPE")
  public static class BidirTopList implements BidirTop {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasOneToManyListJPA parent;

    @OneToMany(mappedBy = "parent", cascade = CascadeType.ALL)
    private List<BidirectionalGrandchildListJPA> bidirGrandchildren = Utils.newArrayList();

    private String childVal;

    public String getId() {
      return id;
    }

    public HasOneToManyListJPA getParent() {
      return parent;
    }

    public void setParent(HasOneToManyJPA parent) {
      this.parent = (HasOneToManyListJPA) parent;
    }

    public List<BidirectionalGrandchildListJPA> getBidirGrandchildren() {
      return bidirGrandchildren;
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

      BidirTopList that = (BidirTopList) o;

      if (id != null ? !id.equals(that.id) : that.id != null) {
        return false;
      }

      return true;
    }

    public int hashCode() {
      return (id != null ? id.hashCode() : 0);
    }

    public int getPropertyCount() {
      return 3;
    }
  }
  
  @Entity
  public static class BidirMiddleList extends BidirTopList implements BidirMiddle {
    private Long middleChildVal;

    public Long getMiddleChildVal() {
      return middleChildVal;
    }

    public void setMiddleChildVal(Long middleChildVal) {
      this.middleChildVal = middleChildVal;
    }
    
    public int getPropertyCount() {
      return 4;
    }

  }
  
  @Entity
  public static class BidirBottomList extends BidirMiddleList implements BidirBottom {
    private Double bottomChildVal;

    public Double getBottomChildVal() {
      return bottomChildVal;
    }

    public void setBottomChildVal(Double bottomChildVal) {
      this.bottomChildVal = bottomChildVal;
    }
    
    public int getPropertyCount() {
      return 5;
    }
  }

  @Entity
  @DiscriminatorValue(value = "T")
  public static class BidirTopLongPkList implements BidirTopLongPk {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasOneToManyLongPkListJPA parent;

    private String childVal;

    public HasOneToManyLongPkJPA getParent() {
      return parent;
    }

    public void setParent(HasOneToManyLongPkJPA parent) {
      this.parent = (HasOneToManyLongPkListJPA) parent;
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

      BidirTopLongPkList that = (BidirTopLongPkList) o;

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
  public static class BidirMiddleLongPkList extends BidirTopLongPkList implements BidirMiddleLongPk {
  }
  
  @Entity
  @DiscriminatorValue(value = "B")
  public static class BidirBottomLongPkList extends BidirMiddleLongPkList implements BidirBottomLongPk {
  }
  
  @Entity
  @DiscriminatorColumn(name = "DISCRIMINATOR", discriminatorType = DiscriminatorType.INTEGER)
  @DiscriminatorValue(value = "1")
  public static class BidirTopStringPkList {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private HasOneToManyStringPkListJPA parent;

    private String childVal;

    public HasOneToManyStringPkListJPA getParent() {
      return parent;
    }

    public void setParent(HasOneToManyStringPkListJPA parent) {
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

      BidirTopStringPkList that = (BidirTopStringPkList) o;

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
  @DiscriminatorValue(value = "2")
  public static class BidirMiddleStringPkList extends BidirTopStringPkList {
    @Column(name = "middlechildval_col")
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
  @DiscriminatorValue(value = "3")
  public static class BidirBottomStringPkList extends BidirMiddleStringPkList {
  }

  @Entity
  @DiscriminatorColumn(name = "TYPE", discriminatorType = DiscriminatorType.CHAR)
  @DiscriminatorValue(value = "A")
  public static class BidirTopUnencodedStringPkList implements BidirTopUnencodedStringPk {
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

      BidirTopUnencodedStringPkList that = (BidirTopUnencodedStringPkList) o;

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
  public static class BidirMiddleUnencodedStringPkList extends BidirTopUnencodedStringPkList implements BidirMiddleUnencodedStringPk {
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
  public static class BidirBottomUnencodedStringPkList extends BidirMiddleUnencodedStringPkList implements BidirBottomUnencodedStringPk {
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
  
  @Entity
  public static class BidirectionalGrandchildListJPA {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @ManyToOne(fetch = FetchType.LAZY)
    private BidirTopList parent;

    private String childVal;

    public BidirTopList getParent() {
      return parent;
    }

    public void setParent(BidirTopList parent) {
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

      BidirectionalGrandchildListJPA that = (BidirectionalGrandchildListJPA) o;

      if (id != null ? !id.equals(that.id) : that.id != null) {
        return false;
      }

      return true;
    }

    public int hashCode() {
      return (id != null ? id.hashCode() : 0);
    }
  }
}
