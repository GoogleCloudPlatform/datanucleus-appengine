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
 * Example 1 (A-X):
 * A has List<X>
 * B extends A
 * X has a back-pointer to A
 * Y extends X
 *
 * Example 2 (B-Y):
 * A
 * B extends A has List<Y extends X>
 * X
 * Y has a back-pointer to A
 *
 * Example 3 (B-X):
 * A
 * B extends A has List<X>
 * X has back-pointer to B
 * Y extends X
 * 
 * Example 4 (A-Y):
 * A has List<Y extends X>
 * B extends A
 * X
 * Y extends X, has a back-pointer to A
 *
 * @author Max Ross <max.ross@gmail.com>
 */
public class BidirectionalOneToManySubclassesJDO {

  public static class Example1 {
    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class A {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Long id;

      private String aString;

      @Element(dependent = "true")
      @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="xString DESC"))
      @Persistent(mappedBy = "parent")
      private List<X> children = new ArrayList<X>();

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getAString() {
        return aString;
      }

      public void setAString(String aString) {
        this.aString = aString;
      }

      public List<X> getChildren() {
        return children;
      }

      public void setChildren(List<X> children) {
        this.children = children;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class B extends A {
      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }
    }

    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class X {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Key id;

      @SuppressWarnings("unused")
      @Persistent
      private A parent;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class Y extends X {
    }
  }

  public static class Example2 {
    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class A {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Long id;

      private String aString;

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
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
    public static class B extends A {
      @Element(dependent = "true")
      @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="xString DESC"))
      @Persistent(mappedBy = "parent")
      private List<Y> children = new ArrayList<Y>();

      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }

      public List<Y> getChildren() {
        return children;
      }

      public void setChildren(List<Y> children) {
        this.children = children;
      }
    }

    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class X {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Key id;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class Y extends X {
      @Persistent
      private B parent;

      private String yString;

      public B getParent() {
        return parent;
      }

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }

  public static class Example3 {
    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class A {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Long id;

      private String aString;

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
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
    public static class B extends A {
      // TODO This ought to be generic of X not Y
      @Element(dependent = "true")
      @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="xString DESC"))
      @Persistent(mappedBy = "parent")
      private List<Y> children = new ArrayList<Y>();

      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }

      public List<Y> getChildren() {
        return children;
      }

      public void setChildren(List<Y> children) {
        this.children = children;
      }
    }

    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class X {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Key id;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class Y extends X{
      // TODO This ought to be on X not Y
      @Persistent
      private B parent;

      public B getParent() {
        return parent;
      }

      public void setParent(B parent) {
        this.parent = parent;
      }

      private String yString;

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }

  public static class Example4 {
    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class A {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Long id;

      private String aString;

      @Element(dependent = "true")
      @Order(extensions = @Extension(vendorName = "datanucleus", key="list-ordering", value="xString DESC"))
      @Persistent(mappedBy = "parent")
      private List<Y> children = new ArrayList<Y>();

      public Long getId() {
        return id;
      }

      public void setId(Long id) {
        this.id = id;
      }

      public String getAString() {
        return aString;
      }

      public void setAString(String aString) {
        this.aString = aString;
      }

      public List<Y> getChildren() {
        return children;
      }

      public void setChildren(List<Y> children) {
        this.children = children;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class B extends A {
      private String bString;

      public String getBString() {
        return bString;
      }

      public void setBString(String bString) {
        this.bString = bString;
      }
    }

    @PersistenceCapable(detachable = "true")
    @Inheritance(customStrategy = "complete-table")
    public static class X {
      @PrimaryKey
      @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
      private Key id;

      private String xString;

      public Key getId() {
        return id;
      }

      public void setId(Key id) {
        this.id = id;
      }

      public String getXString() {
        return xString;
      }

      public void setXString(String xString) {
        this.xString = xString;
      }
    }

    @PersistenceCapable(detachable = "true")
    public static class Y extends X {
      @Persistent
      private A parent;

      private String yString;

      public A getParent() {
        return parent;
      }

      public String getYString() {
        return yString;
      }

      public void setYString(String yString) {
        this.yString = yString;
      }
    }
  }
}