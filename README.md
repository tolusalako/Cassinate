# Cassinate
A hibernate-like support utility for cassandra.


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
