// Copyright 2008 Google Inc. All Rights Reserved.

package com.google.appengine.library;

import java.util.Date;

import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

/**
 * @author kjin@google.com (Kevin Jin)
 *
 */
@PersistenceCapable(identityType = IdentityType.APPLICATION)
public class Book {
  @PrimaryKey
  @Persistent(valueStrategy = IdGeneratorStrategy.IDENTITY)
  private String id;

  @Persistent
  private String category;
  
  @Persistent
  private String lastname;

  @Persistent
  private String firstname;

  @Persistent
  private String title;

  @Persistent
  private Date created;
  
  @Persistent
  private long year;

  public Book(String category, Date created, String firstname, String lastname, String title,
      long year) {
    super();
    this.category = category;
    this.created = created;
    this.firstname = firstname;
    this.lastname = lastname;
    this.title = title;
    this.year = year;
  }
  public String getId() {
    return id;
  }
  public void setCategory(String category) {
    this.category = category;
  }
  public String getCategory() {
    return category;
  }
  public void setLastname(String lastname) {
    this.lastname = lastname;
  }
  public String getLastname() {
    return lastname;
  }
  public void setFirstname(String firstname) {
    this.firstname = firstname;
  }
  public String getFirstname() {
    return firstname;
  }
  public void setTitle(String title) {
    this.title = title;
  }
  public String getTitle() {
    return title;
  }
  public void setCreated(Date created) {
    this.created = created;
  }
  public Date getCreated() {
    return created;
  }
  public void setYear(long year) {
    this.year = year;
  }
  public long getYear() {
    return year;
  }

}
