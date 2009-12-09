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

import javax.jdo.annotations.Column;
import javax.jdo.annotations.Embedded;
import javax.jdo.annotations.EmbeddedOnly;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * There is a ton of duplication in these classes.  The reason is that the
 * runtime enhancer gets unhappy when there are multiple persistent classes
 * extending one persistent class.  So, in order to get all the permutations
 * of inheritance strategies that we want to test, we have to create a large
 * number of very narrow trees.  Annoying.
 *
 * @author Max Ross <maxr@google.com>
 */
public class SubclassesJDO {

  public interface Parent {
    Long getId();
    void setAString(String val);
    String getAString();
  }

  public interface Child extends Parent {
    void setBString(String val);
    String getBString();
  }

  public interface Grandchild extends Child {
    void setCString(String val);
    String getCString();
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class CompleteTableParentWithCompleteTableChild implements Parent {
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
    public static class Child extends CompleteTableParentWithCompleteTableChild
        implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }

      @PersistenceCapable(identityType = IdentityType.APPLICATION)
      @Inheritance(customStrategy = "complete-table")
      public static class Grandchild extends Child implements SubclassesJDO.Grandchild {
        @Persistent
        private String cString;

        public void setCString(String cString) {
          this.cString = cString;
        }

        public String getCString() {
          return cString;
        }
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class CompleteTableParentWithNewTableChild implements Parent {
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
    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
    public static class Child extends CompleteTableParentWithNewTableChild implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class CompleteTableParentWithSubclassTableChild implements Parent {
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
    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
    public static class Child extends CompleteTableParentWithSubclassTableChild
        implements SubclassesJDO.Child {

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
  @Inheritance(customStrategy = "complete-table")
  public static class CompleteTableParentNoChildStrategy implements Parent {
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
    public static class Child
        extends CompleteTableParentNoChildStrategy implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }

      @PersistenceCapable(identityType = IdentityType.APPLICATION)
      public static class Grandchild extends Child implements SubclassesJDO.Grandchild {
        @Persistent
        private String cString;

        public void setCString(String cString) {
          this.cString = cString;
        }
  
        public String getCString() {
          return cString;
        }
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
  public static class NewTableParentWithCompleteTableChild implements Parent {
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
    public static class Child extends NewTableParentWithCompleteTableChild implements
                                                                           SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
  public static class NewTableParentWithNewTableChild implements Parent {
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
    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
    public static class Child extends NewTableParentWithNewTableChild implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
  public static class NewTableParentWithSubclassTableChild implements Parent {
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
    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
    public static class Child extends NewTableParentWithSubclassTableChild implements
                                                                           SubclassesJDO.Child {

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
  public static class SubclassTableParentWithCompleteTableChild implements Parent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private String aString;

    @Persistent
    private String overriddenString;

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
    public static class Child extends SubclassTableParentWithCompleteTableChild
        implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }

      @PersistenceCapable(identityType = IdentityType.APPLICATION)
      @Inheritance(customStrategy = "complete-table")
      public static class Grandchild extends Child implements SubclassesJDO.Grandchild {
        @Persistent
        private String cString;

        public void setCString(String cString) {
          this.cString = cString;
        }

        public String getCString() {
          return cString;
        }
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
  public static class SubclassTableParentWithNewTableChild implements Parent {
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
    @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
    public static class Child extends SubclassTableParentWithNewTableChild implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }

      @PersistenceCapable(identityType = IdentityType.APPLICATION)
      @Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
      public static class Grandchild extends Child implements SubclassesJDO.Grandchild {
        @Persistent
        private String cString;

        public void setCString(String cString) {
          this.cString = cString;
        }

        public String getCString() {
          return cString;
        }
      }
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
  public static class SubclassTableParentWithSubclassTableChild implements Parent {
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
    @Inheritance(strategy = InheritanceStrategy.SUBCLASS_TABLE)
    public static class Child extends SubclassTableParentWithSubclassTableChild
        implements SubclassesJDO.Child {

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

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class OverrideParent implements Parent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    private String aString;

    @Persistent
    private String overriddenString;

    public Long getId() {
      return id;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }

    public String getAString() {
      return aString;
    }

    public String getOverriddenString() {
      return overriddenString;
    }

    public void setOverriddenString(String overriddenString) {
      this.overriddenString = overriddenString;
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    public static class Child extends OverrideParent implements SubclassesJDO.Child {

      @Persistent
      private String bString;

      @Persistent(column = "overridden_string")
      private String overriddenString;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }

      @Override
      public String getOverriddenString() {
        return overriddenString;
      }

      @Override
      public void setOverriddenString(String overriddenString) {
        this.overriddenString = overriddenString;
      }
    }
  }

  @PersistenceCapable
  @EmbeddedOnly
  @Inheritance(customStrategy = "complete-table")
  public static class IsEmbeddedOnlyBase {
    private String val0;

    public String getVal0() {
      return val0;
    }

    public void setVal0(String val0) {
      this.val0 = val0;
    }
  }

  @PersistenceCapable
  @EmbeddedOnly
  public static class IsEmbeddedOnly extends IsEmbeddedOnlyBase {
    private String val1;

    public String getVal1() {
      return val1;
    }

    public void setVal1(String val1) {
      this.val1 = val1;
    }
  }

  @PersistenceCapable
  @EmbeddedOnly
  @Inheritance(customStrategy = "complete-table")
  public static class IsEmbeddedOnlyBase2 {
    private String val2;

    public String getVal2() {
      return val2;
    }

    public void setVal2(String val2) {
      this.val2 = val2;
    }
  }

  @PersistenceCapable
  @EmbeddedOnly
  public static class IsEmbeddedOnly2 extends IsEmbeddedOnlyBase2 {
    private String val3;

    public String getVal3() {
      return val3;
    }

    public void setVal3(String val3) {
      this.val3 = val3;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  @Inheritance(customStrategy = "complete-table")
  public static class CompleteTableParentWithEmbedded implements Parent {
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    private Long id;

    @Persistent
    @Embedded
    private IsEmbeddedOnly embedded;

    @Persistent
    @Embedded(members={
                @Persistent(name="val0", columns=@Column(name="VAL0"))
                })
    private IsEmbeddedOnlyBase embeddedBase;

    private String aString;

    public Long getId() {
      return id;
    }

    public IsEmbeddedOnly getEmbedded() {
      return embedded;
    }

    public void setEmbedded(IsEmbeddedOnly embedded) {
      this.embedded = embedded;
    }

    public String getAString() {
      return aString;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }

    public IsEmbeddedOnlyBase getEmbeddedBase() {
      return embeddedBase;
    }

    public void setEmbeddedBase(IsEmbeddedOnlyBase embeddedBase) {
      this.embeddedBase = embeddedBase;
    }

    @PersistenceCapable(identityType = IdentityType.APPLICATION)
    public static class Child extends CompleteTableParentWithEmbedded {
      @Persistent
      private String bString;

      @Persistent
      @Embedded(members={
                  @Persistent(name="val2", columns=@Column(name="VAL2"))
                  })
      private IsEmbeddedOnlyBase2 embeddedBase2;

      @Persistent
      @Embedded
      private IsEmbeddedOnly2 embedded2;

      public void setBString(String bString) {
        this.bString = bString;
      }

      public String getBString() {
        return bString;
      }

      public IsEmbeddedOnly2 getEmbedded2() {
        return embedded2;
      }

      public void setEmbedded2(IsEmbeddedOnly2 embedded2) {
        this.embedded2 = embedded2;
      }

      public IsEmbeddedOnlyBase2 getEmbeddedBase2() {
        return embeddedBase2;
      }

      public void setEmbeddedBase2(IsEmbeddedOnlyBase2 embeddedBase2) {
        this.embeddedBase2 = embeddedBase2;
      }
    }
  }

  public static class NondurableParent {
    private Long id;

    private String str;

    public Long getId() {
      return id;
    }

    public void setId(Long id) {
      this.id = id;
    }

    public String getStr() {
      return str;
    }

    public void setStr(String str) {
      this.str = str;
    }
  }

  @PersistenceCapable(identityType = IdentityType.APPLICATION)
  public static class DurableChild extends NondurableParent {

    @Override
    @PrimaryKey
    @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
    public Long getId() {
      return super.getId();
    }

    @Override
    @Persistent
    public String getStr() {
      return super.getStr();
    }

    @Override
    public void setId(Long id) {
      super.setId(id);
    }

    @Override
    public void setStr(String str) {
      super.setStr(str);
    }
  }
}