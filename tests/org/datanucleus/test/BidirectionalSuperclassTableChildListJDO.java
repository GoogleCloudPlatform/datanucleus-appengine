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
 package org.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import org.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyJDO;
import org.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyLongPkJDO;
import org.datanucleus.test.HasPolymorphicRelationsJDO.HasOneToManyUnencodedStringPkJDO;
import org.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyList;
import org.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListLongPk;
import org.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListLongPkChildKeyPk;
import org.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListStringPk;
import org.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListUnencodedStringPk;
import org.datanucleus.test.HasPolymorphicRelationsListJDO.HasOneToManyListUnencodedStringPkChildKey;

import javax.jdo.annotations.Discriminator;
import javax.jdo.annotations.DiscriminatorStrategy;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

public class BidirectionalSuperclassTableChildListJDO {
  
  @PersistenceCapable(detachable = "true")
  @Discriminator(column = "DISCRIMINATOR")
  public static class BidirTop implements BidirectionalSuperclassTableChildJDO.BidirTop {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;

    private String childVal;
    
    @Persistent
    private HasOneToManyList parent;
    
    public void setChildVal(String childVal) {
      this.childVal = childVal;
    }

    public String getChildVal() {
      return childVal;
    }

    public void setParent(HasOneToManyJDO parent) {
      this.parent = (HasOneToManyList)parent;
    }

    public HasOneToManyJDO getParent() {
      return this.parent;
    }

    public String getId() {
      return id;
    }

    public int getPropertyCount() {
      return 2; // childVal, DISCRIMINATOR
    }
    
  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirMiddle extends BidirTop implements BidirectionalSuperclassTableChildJDO.BidirMiddle {
    private Long middleChildVal;

    public Long getMiddleChildVal() {
      return middleChildVal;
    }

    public void setMiddleChildVal(Long middleChildVal) {
      this.middleChildVal = middleChildVal;
    }
    
    public int getPropertyCount() {
      return 3; // childVal, middleChildVal, DISCRIMINATOR
    }

  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirBottom extends BidirMiddle implements BidirectionalSuperclassTableChildJDO.BidirBottom {
    private Double bottomChildVal;

    public Double getBottomChildVal() {
      return bottomChildVal;
    }

    public void setBottomChildVal(Double bottomChildVal) {
      this.bottomChildVal = bottomChildVal;
    }

    public int getPropertyCount() {
      return 4; // childVal, middleChildVal, bottomChildVal, DISCRIMINATOR
    }
  }

  @PersistenceCapable(detachable = "true")
  @Discriminator(column = "DISCRIMINATOR")
  public static class BidirTopLongPk implements BidirectionalSuperclassTableChildJDO.BidirTopLongPk {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;

    private String childVal;
    
    @Persistent
    private HasOneToManyListLongPk parent;
    
    public void setChildVal(String childVal) {
      this.childVal = childVal;
    }

    public String getChildVal() {
      return childVal;
    }

    public void setParent(HasOneToManyLongPkJDO parent) {
      this.parent = (HasOneToManyListLongPk)parent;
    }

    public HasOneToManyLongPkJDO getParent() {
      return this.parent;
    }

    public String getId() {
      return id;
    }
    
  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirMiddleLongPk extends BidirTopLongPk implements BidirectionalSuperclassTableChildJDO.BidirMiddleLongPk {
  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirBottomLongPk extends BidirMiddleLongPk implements BidirectionalSuperclassTableChildJDO.BidirBottomLongPk {
  }
  
  @PersistenceCapable(detachable = "true")
  @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
  @Discriminator(strategy = DiscriminatorStrategy.VALUE_MAP, value = "T")
  public static class BidirTopUnencodedStringPk implements BidirectionalSuperclassTableChildJDO.BidirTopUnencodedStringPkJDO {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;

    private String childVal;
    
    @Persistent
    private HasOneToManyListUnencodedStringPk parent;
    
    public void setChildVal(String childVal) {
      this.childVal = childVal;
    }

    public String getChildVal() {
      return childVal;
    }

    public void setParent(HasOneToManyUnencodedStringPkJDO parent) {
      this.parent = (HasOneToManyListUnencodedStringPk)parent;
    }

    public HasOneToManyUnencodedStringPkJDO getParent() {
      return this.parent;
    }

    public String getId() {
      return id;
    }
    
  }
  
  @PersistenceCapable(detachable = "true")
  @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
  @Discriminator(value = "M")
  public static class BidirMiddleUnencodedStringPk extends BidirTopUnencodedStringPk implements BidirectionalSuperclassTableChildJDO.BidirMiddleUnencodedStringPkJDO {

  }

  @PersistenceCapable(detachable = "true")
  @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
  @Discriminator(value = "B")
  public static class BidirBottomUnencodedStringPk extends BidirMiddleUnencodedStringPk implements BidirectionalSuperclassTableChildJDO.BidirBottomUnencodedStringPkJDO {

  }

  @PersistenceCapable(detachable = "true")
  @Discriminator(column = "DISCRIMINATOR")
  public static class BidirTopStringPk {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;

    @Persistent
    private HasOneToManyListStringPk parent;

    @Persistent
    private String childVal;

    public String getId() {
      return id;
    }

    public void setChildVal(String childVal) {
      this.childVal = childVal;
    }

    public String getChildVal() {
      return childVal;
    }

    public void setParent(HasOneToManyListStringPk parent) {
      this.parent = parent;
    }

    public HasOneToManyListStringPk getParent() {
      return this.parent;
    }

  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirMiddleStringPk extends BidirTopStringPk {
    
  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirBottomStringPk extends BidirMiddleStringPk {
    
  }

  @PersistenceCapable(detachable = "true")
  @Discriminator(column = "DISCRIMINATOR")
  public static class BidirTopLongPkChildKey {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    @Persistent
    private HasOneToManyListLongPkChildKeyPk parent;

    public Key getKey() {
      return key;
    }

    public HasOneToManyListLongPkChildKeyPk getParent() {
      return parent;
    }
  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirMiddleLongPkChildKey extends BidirTopLongPkChildKey {
    
  }

  @PersistenceCapable(detachable = "true")
  public static class BidirBottomLongPkChildKey extends BidirMiddleLongPkChildKey {
    
  }

  @PersistenceCapable(detachable = "true")
  @Discriminator(column = "DISCRIMINATOR")
  public static class BidirTopUnencodedStringPkChildKey {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key key;

    @Persistent
    private HasOneToManyListUnencodedStringPkChildKey parent;

    public Key getKey() {
      return key;
    }

    public HasOneToManyListUnencodedStringPkChildKey getParent() {
      return parent;
    }

  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirMiddleUnencodedStringPkChildKey extends BidirTopUnencodedStringPkChildKey {
  
  }
  
  @PersistenceCapable(detachable = "true")
  public static class BidirBottomUnencodedStringPkChildKey extends BidirMiddleUnencodedStringPkChildKey {
  
  }

}
