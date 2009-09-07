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

import com.google.appengine.api.datastore.Key;

import org.datanucleus.store.appengine.Utils;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;

/**
 * @author Max Ross <maxr@google.com>
 */
public class JoinsJPA {

  @Entity
  public static class Student {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int grade;

    @OneToMany
    private List<Course> courses = new ArrayList<Course>();

    @OneToOne(fetch = FetchType.LAZY)
    private Major major;

    public int getGrade() {
      return grade;
    }

    public void setGrade(int grade) {
      this.grade = grade;
    }

    public List<Course> getCourses() {
      return courses;
    }

    public void setCourses(List<Course> courses) {
      this.courses = courses;
    }

    public Major getMajor() {
      return major;
    }

    public void setMajor(Major major) {
      this.major = major;
    }

    public Long getId() {
      return id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Student student = (Student) o;

      if (id != null ? !id.equals(student.id) : student.id != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return id != null ? id.hashCode() : 0;
    }
  }

  @Entity
  public static class Course {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key key;

    private String department;

    public Key getKey() {
      return key;
    }

    public String getDepartment() {
      return department;
    }

    public void setDepartment(String department) {
      this.department = department;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Course course = (Course) o;

      if (key != null ? !key.equals(course.key) : course.key != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return key != null ? key.hashCode() : 0;
    }
  }

  @Entity
  public static class Major {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Key key;

    private String school;

    public Key getKey() {
      return key;
    }

    public void setKey(Key key) {
      this.key = key;
    }

    public String getSchool() {
      return school;
    }

    public void setSchool(String school) {
      this.school = school;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Major major = (Major) o;

      if (key != null ? !key.equals(major.key) : major.key != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (school != null ? school.hashCode() : 0);
      return result;
    }
  }

  public static com.google.appengine.api.datastore.Entity newStudentEntity(
      int grade, com.google.appengine.api.datastore.Entity... courses) {
    return newStudentEntity(null, grade, courses);
  }

  public static com.google.appengine.api.datastore.Entity newStudentEntity(
      com.google.appengine.api.datastore.Entity major, int grade, 
      com.google.appengine.api.datastore.Entity... courses) {
    com.google.appengine.api.datastore.Entity s =
        new com.google.appengine.api.datastore.Entity("JoinsJPA$Student");
    s.setProperty("grade", grade);
    Key majorKey = major == null ? null : major.getKey();
    s.setProperty("major_key", majorKey);
    List<Key> courseKeys = Utils.newArrayList();
    for (com.google.appengine.api.datastore.Entity course : courses) {
      courseKeys.add(course.getKey());
    }
    s.setProperty("courses", courseKeys);
    return s;
  }

  public static com.google.appengine.api.datastore.Entity newCourseEntity(String dept) {
    com.google.appengine.api.datastore.Entity c =
        new com.google.appengine.api.datastore.Entity("JoinsJPA$Course");
    c.setProperty("department", dept);
    return c;
  }

  public static com.google.appengine.api.datastore.Entity newMajorEntity(String school) {
    com.google.appengine.api.datastore.Entity m =
        new com.google.appengine.api.datastore.Entity("JoinsJPA$Major");
    m.setProperty("school", school);
    return m;
  }
}