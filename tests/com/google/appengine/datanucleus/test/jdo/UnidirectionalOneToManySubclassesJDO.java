/*
 * Copyright (C) 2010 Google Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.appengine.datanucleus.test.jdo;

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.List;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.Order;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class UnidirectionalOneToManySubclassesJDO {

  @PersistenceCapable(detachable = "true")
  @Inheritance(customStrategy = "complete-table")
  public static class SuperParentWithSuperChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    private String superParentString;

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="aString DESC"))
    private List<SuperChild> superChildren = new ArrayList<SuperChild>();

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public String getSuperParentString() {
      return superParentString;
    }

    public void setSuperParentString(String superParentString) {
      this.superParentString = superParentString;
    }

    public List<SuperChild> getSuperParentSuperChildren() {
      return superChildren;
    }

    public void setSuperParentSuperChildren(
        List<SuperChild> superChildren) {
      this.superChildren = superChildren;
    }
  }

  @PersistenceCapable(detachable = "true")
  @Inheritance(customStrategy = "complete-table")
  public static class SuperParentWithSubChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    private String superParentString;

    @Element(dependent = "true")
    @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="bString DESC, aString ASC"))
    private List<SubChild> subChildren = new ArrayList<SubChild>();

    public Long getId() {
      return id;
    }

    public String getSuperParentString() {
      return superParentString;
    }

    public void setSuperParentString(String superParentString) {
      this.superParentString = superParentString;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public List<SubChild> getSuperParentSubChildren() {
      return subChildren;
    }

    public void setSuperParentSubChildren(
        List<SubChild> subChildren) {
      this.subChildren = subChildren;
    }
  }

  @PersistenceCapable(detachable = "true")
  public static class SubParentWithSuperChild extends SuperParentWithSuperChild {
    private String subParentString;

    public String getSubParentString() {
      return subParentString;
    }

    public void setSubParentString(String subParentString) {
      this.subParentString = subParentString;
    }
  }

  @PersistenceCapable(detachable = "true")
  public static class SubParentWithSubChild extends SuperParentWithSubChild {
    
    private String subParentString;

    public String getSubParentString() {
      return subParentString;
    }

    public void setSubParentString(String subParentString) {
      this.subParentString = subParentString;
    }
  }

  @PersistenceCapable(detachable = "true")
  @Inheritance(customStrategy = "complete-table")
  public static class SuperChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Key id;

    private String aString;

    public Key getId() {
      return id;
    }

    public void setId(Key id) {
      this.id = id;
    }

    public String getAString() {
      return aString;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }
  }

  @PersistenceCapable(detachable = "true")
  public static class SubChild extends SuperChild {

    private String bString;

    public String getBString() {
      return bString;
    }

    public void setBString(String bString) {
      this.bString = bString;
    }
  }
}
