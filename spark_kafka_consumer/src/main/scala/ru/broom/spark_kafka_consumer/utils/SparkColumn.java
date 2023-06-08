package ru.broom.spark_kafka_consumer.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Simple annotation to extend and ease the Javabean metadata when
 * converting to a Spark column in a dataframe.
 * 
 * @author jgp
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

  /**
   * The name of the column can be overriden.
   */
  String name() default "";

  /**
   * Forces the data type of the column
   */
  String type() default "";

  /**
   * Forces the required/nullable property
   */
  boolean nullable() default true;
}
