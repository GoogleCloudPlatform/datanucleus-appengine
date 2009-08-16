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

import com.google.appengine.api.datastore.Blob;
import com.google.appengine.repackaged.com.google.io.protocol.ProtocolMessage;
import com.google.apphosting.api.DatastorePb;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.appengine.SerializationStrategy;

import java.io.Serializable;
import java.util.List;

import javax.jdo.annotations.Extension;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author Max Ross <maxr@google.com>
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class HasSerializableJDO {

  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private Long id;

  @Persistent(serialized = "true")
  private Yam yam;

  @Persistent(serialized = "true")
  private List<Yam> yamList;

  @Persistent(serialized = "true")
  @Extension(vendorName = "datanucleus", key="serialization-strategy",
             value="org.datanucleus.test.HasSerializableJDO$ProtocolBufferSerializationStrategy")
  private DatastorePb.Query query;

  @Persistent(serialized = "true")
  Integer integer;

  @Persistent(serialized = "true")
  private Flight flight;

  public Yam getYam() {
    return yam;
  }

  public void setYam(Yam yam) {
    this.yam = yam;
  }

  public List<Yam> getYamList() {
    return yamList;
  }

  public void setYamList(List<Yam> yamList) {
    this.yamList = yamList;
  }

  public DatastorePb.Query getQuery() {
    return query;
  }

  public void setQuery(DatastorePb.Query query) {
    this.query = query;
  }

  public Long getId() {
    return id;
  }

  public Integer getInteger() {
    return integer;
  }

  public void setInteger(Integer integer) {
    this.integer = integer;
  }

  public Flight getFlight() {
    return flight;
  }

  public void setFlight(Flight flight) {
    this.flight = flight;
  }

  public static class ProtocolBufferSerializationStrategy implements SerializationStrategy {

    public Blob serialize(Object obj) {
      ProtocolMessage<?> pb = (ProtocolMessage<?>) obj;
      return new Blob(pb.toByteArray());
    }

    public Object deserialize(Blob blob, Class<?> targetClass) {
      if (!ProtocolMessage.class.isAssignableFrom(targetClass)) {
        throw new NucleusException("Not a pb class!");
      }
      ProtocolMessage<?> pb;
      try {
        pb = (ProtocolMessage<?>) targetClass.newInstance();
      } catch (Exception e) {
        throw new NucleusException("Exception trying to instantiate", e);
      }
      pb.parseFrom(blob.getBytes());
      return pb;
    }
  }
  public static class Yam implements Serializable {
    private String str1;
    private String str2;

    public String getStr1() {
      return str1;
    }

    public void setStr1(String str1) {
      this.str1 = str1;
    }

    public String getStr2() {
      return str2;
    }

    public void setStr2(String str2) {
      this.str2 = str2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Yam yam = (Yam) o;

      if (str1 != null ? !str1.equals(yam.str1) : yam.str1 != null) {
        return false;
      }
      if (str2 != null ? !str2.equals(yam.str2) : yam.str2 != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = str1 != null ? str1.hashCode() : 0;
      result = 31 * result + (str2 != null ? str2.hashCode() : 0);
      return result;
    }
  }
}
