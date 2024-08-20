package io.roach.chaos.workload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Inherited
@Documented
@Target(ElementType.TYPE)
@Retention(RUNTIME)
public @interface Note {
    String value();
}
