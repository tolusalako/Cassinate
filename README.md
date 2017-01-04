# Cassinate
A light weight support utility for cassandra. It automatically scans the classpath for classes annotated with [@Table](http://docs.datastax.com/en/drivers/java/2.1/com/datastax/driver/mapping/annotations/Table.html) and updates the keyspace accordingly. CRUD & other native queries should be handled with the [Java Driver](https://github.com/datastax/java-driver)'s [object mapper](https://github.com/datastax/java-driver/tree/3.x/manual/object_mapper).


# Sample Usage
__Basic__
```java
Cassinate.builder().addContactPoint("192.168.0.10") .useKeyspace("keyspaceName").build();
```
__Ignore final fields__
```java
Cassinate.builder().ignoreModifier(Modifier.FINAL).build();
```
__Ignore @JsonIgnore fields__
```java
Cassinate.builder().ignoreAnnotation(JsonIgnore.class).build();
```


# License
[MIT License](https://github.com/tolusalako/Cassinate/blob/master/LICENSE)
