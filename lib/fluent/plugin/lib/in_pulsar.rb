#
# Copyright 2019- zhengwei01
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "fluent/plugin/input"
require 'fluent/plugin/pulsar_client/PulsarClient'
module Fluent
  module Plugin
    class InPulsarParser < Parser
      Fluent::Plugin.register_parser('in_pulsar', self)
      def parse(text)
        # this plugin is dummy implementation not to raise error
        yield nil, nil
      end
    end
    class PulsarInput < Fluent::Plugin::Input
      Fluent::Plugin.register_input("pulsar", self)
      helpers :server, :extract, :compat_parameters
      desc 'Tag of output events.'
      config_param :tag, :string, default: 'debug.access'
      desc 'The port of pulsar.'
      config_param :pulsar_port, :integer, default: 6650
      desc 'The host of pulsar.'
      config_param :pulsar_host, :string, default: '127.0.0.1'
      desc 'The topic of pulsar.'
      config_param :pulsar_topic, :string, default: 'my-topic'
      desc 'The subscription of pulsar.'
      config_param :pulsar_subscription, :string, default: 'sub'
      desc 'The subtype of pulsar.'
      config_param :pulsar_subtype, :integer, default: 1
      desc 'The pull duration of pulsar client.'
      config_param :pull_duration, :string, default: '0.1'
      config_section :parse do
        config_set_default :@type, 'in_pulsar'
      end
      def configure(conf)
        super

      end
      def start
        super
        client = Message::PulsarClient.new()
        client.connect(@pulsar_host, @pulsar_port)
        client.subscribe(@pulsar_topic, @pulsar_subscription, @pulsar_subtype)
        while true do
          m = client.get_message()
          if m != nil
            time = Fluent::Engine.now
            record = {"message_entry_id"=>m.message_entry_id, "message_ledger_id"=>m.message_ledger_id,  "client_created_id"=>m.client_created_id, "message"=>m.message}
            client.ack(m.client_created_id, m.message_ledger_id, m.message_entry_id)
            router.emit(@tag, time, record)
          else
            sleep @pull_duration.to_f
          end
        end
      end
    end
  end
end
