package com.google.appengine.datanucleus.test;

import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;

@MappedSuperclass
public class BaseVersion
{
    @Id
    Long id;

    @Version
    long version;

    public BaseVersion(long id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }
}
