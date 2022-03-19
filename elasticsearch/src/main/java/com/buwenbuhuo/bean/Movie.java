package com.buwenbuhuo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author 不温卜火
 * Create 2022-03-19 0:57
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description: Movie的方法库
 */
@AllArgsConstructor //相当于全参构造
@NoArgsConstructor //相当于空参构造
@Data //相当于get/set/equles/hashcode/toString方法
public class Movie {
    private String id;
    private String name;
}
