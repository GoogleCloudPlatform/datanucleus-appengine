package com.google.appengine.datanucleus.test.jpa;

import javax.persistence.Entity;

@Entity
public class BaseVersionSubclass extends BaseVersion
{
    String name;

    public BaseVersionSubclass(long id, String name) {
        super(id);
        this.name = name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
