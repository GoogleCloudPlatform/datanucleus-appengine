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
package org.datanucleus.test;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
public class SubclassesJDO {

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class CompleteParent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private String aString;

    public Long getId() {
      return id;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }

    public String getAString() {
      return aString;
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(customStrategy = "complete-table")
    public static class CompleteChild extends CompleteParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(customStrategy = "complete-table")
    public static class CompleteGrandchild extends CompleteChild {
      @Persistent
      private String cString;

      public void setCString(String cString) {
        this.cString = cString;
      }

      public String getCString() {
        return cString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
    public static class NewChild extends CompleteParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
    public static class SubChild extends CompleteParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

// Enhancer rejects this
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
//    public static class SuperChild extends CompleteParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
  public static class NewParent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private String aString;

    public Long getId() {
      return id;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }

    public String getAString() {
      return aString;
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(customStrategy = "complete-table")
    public static class CompleteChild extends NewParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
    public static class NewChild extends NewParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
    public static class SubChild extends NewParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

// enhancer rejects this
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
//    public static class SuperChild extends NewParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
  public static class SubParent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private String aString;

    public Long getId() {
      return id;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }

    public String getAString() {
      return aString;
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(customStrategy = "complete-table")
    public static class CompleteChild extends SubParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
    public static class NewChild extends SubParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
    public static class SubChild extends SubParent {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }

// enhancer rejects this
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
//    public static class SuperChild extends SubParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
  }

// rejected by enhancer
//  @PersistenceCapable(identityType = IdentityType.APPLICATION)
//  @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
//  public static class SuperParent {
//    @PrimaryKey
//    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
//    private Long id;
//
//    @Persistent
//    private String aString;
//
//    public Long getId() {
//      return id;
//    }
//
//    public void setAString(String aString) {
//      this.aString = aString;
//    }
//
//    public String getAString() {
//      return aString;
//    }
//
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(customStrategy = "complete-table")
//    public static class CompleteChild extends SuperParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
//
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
//    public static class NewChild extends SuperParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
//
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
//    public static class SubChild extends SuperParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
//
//    @PersistenceCapable(identityType = IdentityType.APPLICATION)
//    @Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
//    public static class SuperChild extends SuperParent {
//
//      @Persistent
//      private String bString;
//
//      public void setBString(String bString) {
//        this.bString = bString;
//      }
//
//      public String getBString() {
//        return bString;
//      }
//    }
//  }
}