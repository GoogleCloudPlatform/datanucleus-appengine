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

import com.google.appengine.api.datastore.KeyFactory;

import org.datanucleus.jpa.annotations.Extension;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

public class UnidirectionalSingeTableChildJPA {
  
  public static final String DISCRIMINATOR_TOP = "Top";
  public static final String DISCRIMINATOR_MIDDLE = "Middle";
  public static final String DISCRIMINATOR_BOTTOM = "Bottom";
  
  @Entity
  @DiscriminatorValue(value = DISCRIMINATOR_TOP)
  public static class UnidirTop {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Extension(vendorName="datanucleus", key="gae.encoded-pk", value="true")
    private String id;
    
    private String str;
    private String name;

    public UnidirTop(String namedKey) {
      this.id = namedKey == null ? null :
                KeyFactory.keyToString(KeyFactory.createKey(UnidirectionalSingeTableChildJPA.getEntityKind(UnidirTop.class), namedKey));
    }

    public void setId(String id) {
      this.id = id;
    }

    public UnidirTop() {
      this(null);
    }

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

    @Override
    public String toString() {
      return "\n\nid: " + id + "\nstr: " + str + "\nname: " + name;
    }

@Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      UnidirTop book = (UnidirTop) o;

      if (id != null ? !id.equals(book.id) : book.id != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return (id != null ? id.hashCode() : 0);
    }
  }
  
  @Entity
  @DiscriminatorValue(value = DISCRIMINATOR_MIDDLE)
  public static class UnidirMiddle extends UnidirTop {
    public UnidirMiddle() {
      this(null);
    }
    
    public UnidirMiddle(String namedKey) {
      super(namedKey);
    }
  }
  
  @Entity
  @DiscriminatorValue(value = DISCRIMINATOR_BOTTOM)
  public static class UnidirBottom extends UnidirMiddle {
    public UnidirBottom(String namedKey) {
      super(namedKey);
    }
  }
  
  public static String getEntityKind(Class<?> clazz) {
    return clazz.getName().substring(clazz.getPackage().getName().length() + 1);
  }
}
