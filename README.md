<h1 align="center">fastbus</h1>

<p align="center">
  <a href="https://github.com/tsukure/fastbus/releases/latest"><img src="https://img.shields.io/github/v/release/tsukure/fastbus?style=flat-square" alt="latest release"></a>
  <a href="https://maven.tsuku.re/releases/re/tsuku/fastbus/"><img src="https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Fmaven.tsuku.re%2Freleases%2Fre%2Ftsuku%2Ffastbus%2Fmaven-metadata.xml&style=flat-square&label=maven" alt="maven"></a>
  <a href="https://github.com/tsukure/fastbus/actions/workflows/release.yml"><img src="https://img.shields.io/github/actions/workflow/status/tsukure/fastbus/release.yml?branch=main&style=flat-square" alt="release workflow"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/tsukure/fastbus?style=flat-square" alt="license"></a>
</p>

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

```java
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

```java
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

```java
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

```java
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

format sources:

```bash
./mvnw formatter:format
```

check formatting:

```bash
./mvnw formatter:validate
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

rough local jmh numbers, averaged from a few short runs on jdk 21:

| benchmark                                 |  throughput |
|-------------------------------------------|------------:|
| fastbus direct post                       | ~220m ops/s |
| fastbus annotated post                    | ~165m ops/s |
| greenrobot eventbus post                  |  ~16m ops/s |
| guava eventbus post                       |  ~10m ops/s |
| fastbus direct subscribe/unsubscribe      | ~6.5m ops/s |
| guava eventbus subscribe/unsubscribe      | ~5.5m ops/s |
| fastbus annotated subscribe/unsubscribe   | ~900k ops/s |
| greenrobot eventbus subscribe/unsubscribe | ~200k ops/s |

your mileage may vary a bit depending on jvm, hardware, and listener shape.

## license

mit
