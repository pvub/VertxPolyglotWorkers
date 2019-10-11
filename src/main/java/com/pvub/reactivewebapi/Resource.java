package com.pvub.reactivewebapi;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple resource class
 * @author Udai
 */
public class Resource {
 
  private final int id;
 
  private String title;
 
  private String url;
 
  public Resource(int id, String title, String url) {
      this.id = id;
      this.title = title;
      this.url = url;
  }
 
  public Resource(int id) {
      this.id = id;
  }
 
  public int getId() {
      return id;
  }
 
  public String getTitle() {
      return title;
  }
 
  public Resource setTitle(String title) {
      this.title = title;
      return this;
  }
 
  public String getUrl() {
      return url;
  }
 
  public Resource setUrl(String url) {
      this.url = url;
      return this;
  }
}