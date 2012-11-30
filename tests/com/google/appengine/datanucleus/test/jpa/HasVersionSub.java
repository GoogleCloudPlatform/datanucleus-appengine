package com.google.appengine.datanucleus.test.jpa;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Version;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

@Entity
public class HasVersionSub implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    Key key;

    @Version
    protected long version;

    protected int value;

    public void setKey(Key parentKey, String id) {
      this.key = KeyFactory.createKey(parentKey, HasVersionSub.class.getSimpleName(), id);
    }

    public void incValue(int step) { 
      this.value += step; 
    }

    public long getVersion() {
      return version;
    }

    public int getValue() {
      return value;
    }

    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof HasVersionSub)) {
        return false;
      }
      HasVersionSub other = (HasVersionSub)obj;
      if (other.key == null || key == null) {
        return false;
      }
      return key.equals(other.key);
    }

    public int hashCode() {
      if (key == null) {
        return 0;
      }
      return key.hashCode();
    }

    
}