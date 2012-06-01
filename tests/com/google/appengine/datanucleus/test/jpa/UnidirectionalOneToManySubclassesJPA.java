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
package com.google.appengine.datanucleus.test.jpa;

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.OneToMany;
import javax.persistence.OrderBy;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class UnidirectionalOneToManySubclassesJPA {

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class SuperParentWithSuperChild {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Long id;

    private String superParentString;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @OrderBy(value = "aString desc")
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

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class SuperParentWithSubChild {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Long id;

    private String superParentString;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @OrderBy(value = "aString desc")
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

  @Entity
  public static class SubParentWithSuperChild extends SuperParentWithSuperChild {
    private String subParentString;

    public String getSubParentString() {
      return subParentString;
    }

    public void setSubParentString(String subParentString) {
      this.subParentString = subParentString;
    }
  }

  @Entity
  public static class SubParentWithSubChild extends SuperParentWithSubChild {

    private String subParentString;

    public String getSubParentString() {
      return subParentString;
    }

    public void setSubParentString(String subParentString) {
      this.subParentString = subParentString;
    }
  }

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class SuperChild {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
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

  @Entity
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