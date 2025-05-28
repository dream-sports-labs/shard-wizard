package com.dream11.shardwizard.utils;

/**
 * Functional interface to handle exceptions in lambda expressions
 */
@FunctionalInterface
public interface ExceptionalFunction<T, R> {

  R apply(T t) throws Exception;

}