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
package com.google.appengine.datanucleus.test.jpa;

import com.google.appengine.api.datastore.Key;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.CascadeType;
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
public class OwnedJoinsJPA {

  @Entity
  public static class Student {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private int grade;

    @OneToMany(cascade = CascadeType.ALL)
    private List<Course> courses = new ArrayList<Course>();

    @OneToOne(fetch = FetchType.LAZY, cascade = CascadeType.ALL)
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

  public static Student newStudent(int grade, Course... courses) {
    return newStudent(grade, null, courses);
  }

  public static Student newStudent(int grade, Major major, Course... courses) {
    Student s = new Student();
    s.setGrade(grade);
    s.setCourses(Arrays.asList(courses));
    if (major != null) {
      s.setMajor(major);
    }
    return s;
  }

  public static Course newCourse(String dept) {
    Course c = new Course();
    c.setDepartment(dept);
    return c;
  }

  public static Major newMajor(String school) {
    Major m = new Major();
    m.setSchool(school);
    return m;
  }
}