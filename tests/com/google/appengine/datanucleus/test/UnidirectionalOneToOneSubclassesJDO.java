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
package com.google.appengine.datanucleus.test;

import com.google.appengine.api.datastore.Key;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <max.ross@gmail.com>
 */
public class UnidirectionalOneToOneSubclassesJDO {

  @PersistenceCapable(detachable = "true")
  @Inheritance(customStrategy = "complete-table")
  public static class SuperParentWithSuperChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    private String superParentString;

    @Persistent(dependent = "true")
    private SuperChild superChild;

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

    public SuperChild getSuperParentSuperChild() {
      return superChild;
    }

    public void setSuperParentSuperChild(SuperChild superChild) {
      this.superChild = superChild;
    }
  }

  @PersistenceCapable(detachable = "true")
  @Inheritance(customStrategy = "complete-table")
  public static class SuperParentWithSubChild {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    private String superParentString;

    @Persistent(dependent = "true")
    private SubChild subChild;

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

    public SubChild getSuperParentSubChild() {
      return subChild;
    }

    public void setSuperParentSubChild(SubChild subChild) {
      this.subChild = subChild;
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