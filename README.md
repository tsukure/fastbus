# fastbus

`fastbus` is a small java event bus for projects that need simple typed events without a framework.

## install

```xml
<repositories>
  <repository>
    <id>tsukure-releases</id>
    <url>https://maven.tsuku.re/releases</url>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>re.tsuku</groupId>
    <artifactId>fastbus</artifactId>
    <version>1.0.0</version>
  </dependency>
</dependencies>
```

for snapshots, use:

```xml
<repository>
  <id>tsukure-snapshots</id>
  <url>https://maven.tsuku.re/snapshots</url>
</repository>
```

## basic usage

create an event:

```text
import re.tsuku.fastbus.Event;

public final class LoginEvent implements Event {
    private final String username;

    public LoginEvent(String username) {
        this.username = username;
    }

    public String username() {
        return username;
    }
}
```

subscribe with annotated methods:

```text
import re.tsuku.fastbus.FastBus;
import re.tsuku.fastbus.Subscribe;

public final class LoginLogger {
    @Subscribe
    public void onLogin(LoginEvent event) {
        System.out.println(event.username() + " logged in");
    }
}

FastBus bus = new FastBus();
LoginLogger logger = new LoginLogger();

bus.subscribe(logger);
bus.post(new LoginEvent("rin"));
bus.unsubscribe(logger);
```

subscribe with listener functions:

```text
import re.tsuku.fastbus.EventPriority;
import re.tsuku.fastbus.FastBus;
import re.tsuku.fastbus.Subscription;

FastBus bus = new FastBus();

Subscription subscription = bus.subscribe(
    LoginEvent.class,
    event -> System.out.println(event.username()),
    EventPriority.HIGH
);

bus.post(new LoginEvent("rin"));
subscription.unsubscribe();
```

cancelable events can extend `CancellableEvent`:

```text
import re.tsuku.fastbus.CancellableEvent;

public final class MessageEvent extends CancellableEvent {
    private final String message;

    public MessageEvent(String message) {
        this.message = message;
    }

    public String message() {
        return message;
    }
}
```

## build

```bash
./mvnw clean test
```

## benchmarks

build the jmh benchmark jar:

```bash
./mvnw -Pbenchmarks clean package -DskipTests
```

run the benchmark suite:

```bash
java -jar target/benchmarks.jar
```

for a shorter local run:

```bash
java -jar target/benchmarks.jar 'post_.*|subscribe_.*' -wi 2 -i 3 -f 1
```

## license

mit
