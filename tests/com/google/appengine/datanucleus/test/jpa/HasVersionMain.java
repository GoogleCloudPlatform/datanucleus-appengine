package com.google.appengine.datanucleus.test.jpa;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Version;

import com.google.appengine.api.datastore.Key;

@Entity
public class HasVersionMain implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    protected Key id;

    @Version
    protected long version;

    @OneToMany(cascade=CascadeType.ALL, fetch=FetchType.LAZY, orphanRemoval=true)
    protected List<HasVersionSub> subs = new ArrayList<HasVersionSub>();

    public HasVersionMain(Key key) {
      this.id = key;
    }

    public long getVersion() {
      return version;
    }

    public List<HasVersionSub> getSubs() {
      return subs;
    }
}