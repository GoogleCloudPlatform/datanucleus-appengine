package org.datanucleus.test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GenerationType;
import javax.persistence.GeneratedValue;

@Entity
public class Book {
  @Id
  @GeneratedValue(strategy= GenerationType.IDENTITY)
  private String id;
  private String title;
  private String author;
  private String isbn;

  public String getId() {
    return id;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getAuthor() {
    return author;
  }

  public void setAuthor(String author) {
    this.author = author;
  }

  public String getIsbn() {
    return isbn;
  }

  public void setIsbn(String isbn) {
    this.isbn = isbn;
  }

  public String toString() {
    return "\n\nid: " + id + "\ntitle: " + title + "\nauthor: " + author + "\nisbn: " + isbn;
  }

  public static com.google.apphosting.api.datastore.Entity newBookEntity(String author, String isbn,
      String title) {
    com.google.apphosting.api.datastore.Entity e =
        new com.google.apphosting.api.datastore.Entity(Book.class.getName());
    e.setProperty("author", author);
    e.setProperty("isbn", isbn);
    e.setProperty("title", title);
    return e;
  }
}
