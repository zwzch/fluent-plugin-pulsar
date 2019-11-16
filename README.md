# fluent-plugin-pulsar

[Fluentd](https://fluentd.org/) input plugin to do 
as pulsar sink
## Installation

### RubyGems
```
gem install bundler
gem install fluentd
gem install digest-crc
gem install ruby_protobuf
```
## Compile Pulsar API protocol buffer

1. Clone proto file(PulsarApi.proto) from Pulsar Project page.
```
git clone https://github.com/apache/pulsar.git
```
(The proto file path is at pulsar-common/src/main/proto/PulsarApi.proto)


2. Compile the proto file using rprotoc
```
rprotoc PulsarApi.proto
```

3. Move PulsarApi.pb.rb to your project directory.

4. copy lib/fluent/plugin/lib/* to your plugin dir.
## Configuration
```
example:
$ fluent.conf
<source>
  @type pulsar
  @id pulsar_input
  tag debug.access
  pulsar_port 6650
  pulsar_host 127.0.0.1
  pulsar_topic my-topic
  pulsar_subscription sub
  pull_duration 0.1
</source>
<match debug.**>
    @type stdout
    @id stdout_output
</match>
```

## Copyright

* Copyright(c) 2019- zhengwei01
* License
  * Apache License, Version 2.0

## Thanks

+ [ruby-pulsar-client](https://github.com/hiroakiwater/ruby-pulsar-client)
