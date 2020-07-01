package org.apache.cassandra.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD})
@Repeatable(ReplacesList.class)
@interface Replaces
{
    String oldName();

    Class<? extends Converter> converter() default Converter.IdentityConverter.class;

    String scheduledRemoveBy() default "";
}
