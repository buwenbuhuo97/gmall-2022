package com.buwenbuhuo.gmallpublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

/**
 * Author 不温卜火
 * Create 2022-03-21 1:16
 * MyBlog https://buwenbuhuo.blog.csdn.net
 * Description:
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Stat {
    List<Option> options;
    String title;
}

