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
package com.google.appengine.datanucleus.test.jdo;

import java.util.Date;

import javax.jdo.annotations.Discriminator;
import javax.jdo.annotations.DiscriminatorStrategy;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

public class UnidirectionalSuperclassTableChildJDO {
  
  public static final String DISCRIMINATOR_TOP = "Top";
  public static final String DISCRIMINATOR_MIDDLE = "Middle";
  public static final String DISCRIMINATOR_BOTTOM = "Bottom";
    
  @PersistenceCapable(detachable = "true")
  @Discriminator(strategy = DiscriminatorStrategy.VALUE_MAP, column = "TYPE", value = DISCRIMINATOR_TOP)
  public static class UnidirTop {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;
    
    private String str;
    
    private String name;

    public String getId() {
      return id;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setId(String id) {
      this.id = id;
    }

    /**
     * We get weird test failures if we give Flight an equals() method due to
     * attempts to read fields of deleted objects. TODO(maxr) Straighten this
     * out.
     */
    public boolean customEquals(Object o) {
      if (this == o) {
	return true;
      }
      if (o == null || getClass() != o.getClass()) {
	return false;
      }

      UnidirTop unidir = (UnidirTop) o;

      if (str != null ? !str.equals(unidir.str) : unidir.str != null) {
	return false;
      }
      if (name != null ? !name.equals(unidir.name) : unidir.name != null) {
	return false;
      }

      return true;
    }
    
    public int getPropertyCount() {
      return 3;	// str, name, TYPE
    }
  }
  
  @PersistenceCapable(detachable = "true")
  @Discriminator(value = DISCRIMINATOR_MIDDLE)
  public static class UnidirMiddle extends UnidirTop {
    private Date date;

    public Date getDate() {
      return date;
    }

    public void setDate(Date date) {
      this.date = date;
    }

    public int getPropertyCount() {
      return 4;	// str, name, date, TYPE
    }
  }
  
  @PersistenceCapable(detachable = "true")
  @Discriminator(value = DISCRIMINATOR_BOTTOM)
  public static class UnidirBottom extends UnidirMiddle {
    private Integer integer;

    public Integer getInteger() {
      return integer;
    }

    public void setInteger(Integer integer) {
      this.integer = integer;
    }

    public int getPropertyCount() {
      return 5;	// str, name, date, integer, TYPE
    }
}
  
  
  @PersistenceCapable(detachable = "true")
  public static class UnidirTopWithIndexColumn {

    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;
    
    private String str;
    
    private String name;

    @SuppressWarnings("unused")
    private int index;

    public String getId() {
      return id;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
    
  }
  
  @PersistenceCapable
  public static class UnidirTopWithOverriddenIdColumn {
    @SuppressWarnings("unused")
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY, column = "something_else")
    @Extension(vendorName = "datanucleus", key = "gae.encoded-pk", value="true")
    private String id;
  }

  @PersistenceCapable(detachable = "true")
  @Discriminator(column = "TYPE")
  public static class UnidirTopEncodedStringPkSeparateNameField {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    @Extension(vendorName = "datanucleus", key="gae.encoded-pk", value="true")
    private String id;

    @Persistent
    @Extension(vendorName = "datanucleus", key="gae.pk-name", value="true")
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
  
  @PersistenceCapable(detachable = "true")
  public static class UnidirMiddleEncodedStringPkSeparateNameField extends UnidirTopEncodedStringPkSeparateNameField {
  }

  @PersistenceCapable(detachable = "true")
  public static class UnidirBottomEncodedStringPkSeparateNameField extends UnidirMiddleEncodedStringPkSeparateNameField {
  }
  
}
