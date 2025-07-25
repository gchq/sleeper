Coding Conventions
==================

There are a number of ways we tend to write and structure our code, where if we keep them consistent it might improve
the maintainability of the codebase as a whole. We try to keep to these conventions unless there's a good reason within
a specific part of the code. We try to remain open to alternative approaches if they improve maintainability.

## Tests

Please see [our test strategy](test-strategy.md), which also includes conventions when writing tests.

## Java keyword usage

### Access level modifiers

Within a class we try to declare the minimum access level for all elements. Instance fields are almost always private,
with getters added only when necessary. Methods should only be public when they are intended to be called from outside
the class. Usually if a method has no usages from outside a class it can either be private or be deleted. Nested classes
can be private, but builders are usually public.

Classes are usually public, but may be at a lower access level for implementation details.

In a class defining JUnit tests, access level modifiers may be omitted based on preference. Other test helper or test
base classes should follow the same style as non-test code.

### Use of final

We usually only use the final keyword in field declarations. Fields should be declared final in all cases unless they
need to be reassigned. In practice, most fields should be final, as we usually create a new object if a field would
otherwise need to be reassigned.

We don't use final for parameters or local variables. We don't usually declare final classes.

In a class defining JUnit tests, the final keyword may be omitted based on preference. Other test helper or test base
classes should follow the same style as non-test code.

### Use of this

We usually only use the this keyword in a constructor or a setter. If the this keyword is needed to assign any of the
fields in a constructor, we use this in all the field assignments.

For example:

```java
public Partition(String id, Range range) {
    this.id = id;
    this.region = new Region(List.of(range));
}
```

With a builder, the this keyword can be omitted:

```java
public Partition(Builder builder) {
    id = builder.id;
    region = builder.region;
}
```

## Ordering within a Java class

We try to keep to this ordering of elements in a class declaration:

1. Static fields
2. Instance fields
3. Constructors
4. Static methods that return an instance of the class (static constructors)
5. Static methods that return a builder of the class
6. Other public static methods
7. Public instance methods other than those mentioned below
8. Private methods
9. Public getter instance methods that return the value of a field with no other code
10. In a builder, a build method that creates an instance of the class being built
11. Implementations of equals, hashCode, toString when needed (on data classes, not on builders)
12. Nested classes/interfaces

Within this ordering, methods should be in the order that they are expected to be used.

Note that many classes will not contain many of these elements.

## Javadoc

We try to ensure that all classes have Javadoc. Most methods should also have Javadoc. Javadoc should generally explain
purpose, usage and high level behaviour, and avoid implementation details.

An exception to this is test classes, e.g. SomeFeatureTest, SomeFeatureIT, SomeFeatureST (unit, integration and system
tests). These classes should not have Javadoc, to put focus on the tests themselves. Other test code should have
Javadoc, e.g. test helpers, test fakes.

Private methods, as well as getters and setters can be skipped unless there's something important to know. Constructors
should not usually have Javadoc. The class Javadoc usually covers this, as we tend to avoid having more than one
constructor.

Many classes have a static method `builder` that takes no arguments and returns a builder, and this does not usually
need Javadoc. Each builder has a method `build` that creates an instance of the class being built, and this does not
usually need Javadoc.

### Style

Please follow Oracle's standards for Javadoc:
<https://www.oracle.com/technical-resources/articles/java/javadoc-tool.html>

Please pay particular attention to the style guide section in that article.

Javadoc should explain higher level structure and intention, and how to use the code. Please try to talk about the
behaviour of the item being described, and avoid reference to implementation details. Try to avoid information that
would become incorrect if we replaced the implementation but achieved the same behaviour for the consumer. We can make
an exception if a subsystem is complicated enough to require detailed explanation, but it should be something that can
be switched out.

Please avoid using Sleeper class, variable or field names directly except in a link tag, like `{@link SleeperClient}`.
Without a link tag those references would not be updated if they are renamed. Link tags should generally only be used
for classes that are already imported or defined in the current file. Please use them sparingly, and prefer to talk
about domain concepts instead, unless specifically discussing the structure of the code.

The first sentence in a Javadoc comment will be used as a summary fragment in generated documentation. This should not
contain any links or formatting, to read normally as an item in a list.

Checkstyle checks for a lot of our criteria, and should be enabled as an IDE extension.

A notable omission from the Checkstyle checks is capitalisation of descriptions under tags, eg. parameter tags for
methods. Following the Oracle standards, these should be either a short phrase in all lower case, or a full sentence
with the first word capitalised and a full stop. For example:

```java
/**
 * Processes a foo and a bar.
 *
 * @param foo the foo
 * @param bar This is the bar. It must not be null or an empty string.
 */
public void process(String foo, String bar) {
}
```
