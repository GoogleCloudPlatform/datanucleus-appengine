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

import javax.persistence.AttributeOverride;
import javax.persistence.AttributeOverrides;
import javax.persistence.Column;
import javax.persistence.Embeddable;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import javax.persistence.MappedSuperclass;

/**
 * @author Max Ross <maxr@google.com>
 */
public class SubclassesJPA {

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

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class TablePerClass implements Parent {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    private Long id;

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
  }

  @Entity
  public static class TablePerClassChild extends TablePerClass implements Child {
    private String bString;

    public void setBString(String bString) {
      this.bString = bString;
    }

    public String getBString() {
      return bString;
    }
  }

  @Entity
  public static class TablePerClassGrandchild extends TablePerClassChild implements Grandchild {
    private String cString;

    public void setCString(String cString) {
      this.cString = cString;
    }

    public String getCString() {
      return cString;
    }
  }

  @Entity
  @Inheritance(strategy = InheritanceType.JOINED)
  public static class Joined implements Parent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

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
  }

  @Entity
  public static class JoinedChild extends Joined implements Child {
    private String bString;

    public void setBString(String bString) {
      this.bString = bString;
    }

    public String getBString() {
      return bString;
    }
  }

  @Entity
  @Inheritance(strategy = InheritanceType.SINGLE_TABLE)
  public static class SingleTable implements Parent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

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
  }

  @Entity
  public static class SingleTableChild extends SingleTable implements Child {
    private String bString;

    public void setBString(String bString) {
      this.bString = bString;
    }

    public String getBString() {
      return bString;
    }
  }

  @MappedSuperclass
  public static class MappedSuperclassParent implements Parent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String aString;
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
  }

  @Entity
  @AttributeOverride(name = "overriddenString", column = @Column(name = "overridden_string"))
  public static class MappedSuperclassChild extends MappedSuperclassParent implements Child {
    private String bString;

    public void setBString(String bString) {
      this.bString = bString;
    }

    public String getBString() {
      return bString;
    }
  }

  @Entity
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class TablePerClassParentWithEmbedded implements Parent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Embedded
    private IsEmbeddedWithEmbeddedSuperclass embedded;

    @Embedded
    @AttributeOverrides({@AttributeOverride(name="val0", column=@Column(name="VAL0"))})
    private IsEmbeddedBase embeddedBase;

    private String aString;

    public Long getId() {
      return id;
    }

    public IsEmbeddedWithEmbeddedSuperclass getEmbedded() {
      return embedded;
    }

    public void setEmbedded(IsEmbeddedWithEmbeddedSuperclass embedded) {
      this.embedded = embedded;
    }

    public String getAString() {
      return aString;
    }

    public void setAString(String aString) {
      this.aString = aString;
    }

    public IsEmbeddedBase getEmbeddedBase() {
      return embeddedBase;
    }

    public void setEmbeddedBase(IsEmbeddedBase embeddedBase) {
      this.embeddedBase = embeddedBase;
    }
  }

  @Entity
  public static class ChildEmbeddedInTablePerClass extends TablePerClassParentWithEmbedded {
    private String bString;

    @Embedded
    @AttributeOverrides({@AttributeOverride(name="val2", column=@Column(name="VAL2"))})
    private IsEmbeddedBase2 embeddedBase2;

    @Embedded
    private IsEmbeddedWithEmbeddedSuperclass2 embedded2;

    public void setBString(String bString) {
      this.bString = bString;
    }

    public String getBString() {
      return bString;
    }

    public IsEmbeddedWithEmbeddedSuperclass2 getEmbedded2() {
      return embedded2;
    }

    public void setEmbedded2(IsEmbeddedWithEmbeddedSuperclass2 embedded2) {
      this.embedded2 = embedded2;
    }

    public IsEmbeddedBase2 getEmbeddedBase2() {
      return embeddedBase2;
    }

    public void setEmbeddedBase2(IsEmbeddedBase2 embeddedBase2) {
      this.embeddedBase2 = embeddedBase2;
    }
  }

  @Embeddable
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class IsEmbeddedBase2 {

    private String val2;

    public String getVal2() {
      return val2;
    }

    public void setVal2(String val2) {
      this.val2 = val2;
    }
  }

  @Embeddable
  @Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
  public static class IsEmbeddedBase {

    private String val0;

    public String getVal0() {
      return val0;
    }

    public void setVal0(String val0) {
      this.val0 = val0;
    }
  }
}