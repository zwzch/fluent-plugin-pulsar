# Copyright 2017 Hiroaki Kawata
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

require 'socket'
require 'fluent/plugin/lib/pulsar_client/PulsarApi.pb'
require 'digest/crc32c'

module Message

class PulsarMessage
	attr_accessor :client_created_id
	attr_accessor :message_ledger_id
	attr_accessor :message_entry_id
	attr_accessor :message
	
	def initialize()
		@client_created_id = 0
		@message_ledger_id = 0
		@message_entry_id = 0
		@message = ""
	end
end

class PulsarClient

	def initialize()
		@client_created_id = 1
		@request_id = 1
		@producer_id = 1
		@producer_name = "test"
	end
	
	def connect_socket(host, port)
		@sock = Socket.new(Socket::AF_INET, Socket::SOCK_STREAM, 0)
		sockaddr = Socket.sockaddr_in(port, host)
		@sock.connect(sockaddr)	
	end

	def close_socket()
		@sock.close
	end

	def command_connect()
		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::CONNECT
			)

		base_command.connect = Pulsar::Proto::CommandConnect.new
		base_command.connect.client_version 	= "ruby-client-0.0.2"
		base_command.connect.protocol_version 	= 6
	
		byte_cmd = base_command.serialize_to_string()
		total_length = byte_cmd.length + 4

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd

		@sock.write(total_frame)

		recv_length = @sock.read(4).unpack('N')[0]

		recv_cmd_length = @sock.read(4).unpack('N')[0]

		recv_cmd_byte = @sock.read(recv_cmd_length)
		
		recv_cmd = Pulsar::Proto::BaseCommand.new
		recv_cmd.parse_from_string(recv_cmd_byte)


		if recv_cmd.type == Pulsar::Proto::BaseCommand::Type::CONNECTED then
			return true
		else
			return false
		end
	end

	def command_producer(topic)
		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::PRODUCER
			)

		base_command.producer = Pulsar::Proto::CommandProducer.new
		base_command.producer.topic = topic
		base_command.producer.producer_id = @producer_id
		base_command.producer.request_id = @request_id
	
	
		byte_cmd = base_command.serialize_to_string()
		total_length = byte_cmd.length + 4

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd

		@sock.write(total_frame)

		recv_length = @sock.read(4).unpack('N')[0]

		recv_cmd_length = @sock.read(4).unpack('N')[0]

		recv_cmd_byte = @sock.read(recv_cmd_length)
		
		recv_cmd = Pulsar::Proto::BaseCommand.new
		recv_cmd.parse_from_string(recv_cmd_byte)

		if recv_cmd.type == Pulsar::Proto::BaseCommand::Type::PRODUCER_SUCCESS then
			return true	
		else
			return false	
		end
	end

	def command_send(message)
		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::SEND
			)

		base_command.send = Pulsar::Proto::CommandSend.new
		base_command.send.producer_id = @producer_id
		base_command.send.sequence_id = 0
		base_command.send.num_messages = 1
	
		byte_cmd = base_command.serialize_to_string()


		magic_number= [0x0e, 0x01].pack('C*')
		metadata = Pulsar::Proto::MessageMetadata.new
		metadata.producer_name = @producer_name
		metadata.sequence_id = 0
		metadata.publish_time = Time.now.to_i * 1000

		byte_meta = metadata.serialize_to_string()

		meta_payload = [byte_meta.length].pack('N') + byte_meta + message.bytes.pack('C*')

		crc = Digest::CRC32c.new
		crc << meta_payload
		checksum = crc.checksum

		total_length = 4 + byte_cmd.length + 6 + meta_payload.length

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd + magic_number + [checksum].pack('N') + meta_payload

		@sock.write(total_frame)

		recv_length = @sock.read(4).unpack('N')[0]

		recv_cmd_length = @sock.read(4).unpack('N')[0]

		recv_cmd_byte = @sock.read(recv_cmd_length)
		
		recv_cmd = Pulsar::Proto::BaseCommand.new
		recv_cmd.parse_from_string(recv_cmd_byte)

		if recv_cmd.type == Pulsar::Proto::BaseCommand::Type::PRODUCER_SUCCESS then
			return true
		else
			return false	
		end
	end

	def command_lookup(topic)

		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::LOOKUP
			)

		base_command.lookupTopic = Pulsar::Proto::CommandLookupTopic.new
		base_command.lookupTopic.topic = topic
		base_command.lookupTopic.request_id = @request_id
	
		byte_cmd = base_command.serialize_to_string()
		total_length = byte_cmd.length + 4

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd

		@sock.write(total_frame)

		recv_length = @sock.read(4).unpack('N')[0]

		recv_cmd_length = @sock.read(4).unpack('N')[0]

		recv_cmd_byte = @sock.read(recv_cmd_length)
		
		recv_cmd = Pulsar::Proto::BaseCommand.new
		recv_cmd.parse_from_string(recv_cmd_byte)


		if recv_cmd.type == Pulsar::Proto::BaseCommand::Type::LOOKUP_RESPONSE then
			return true
		else
			print("type:" + recv_cmd.type.to_s + "\n")
			return false
		end
	end

	def command_subscribe(topic, subscription, sub_type)
		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::SUBSCRIBE
			)

		base_command.subscribe = Pulsar::Proto::CommandSubscribe.new
		base_command.subscribe.topic = topic
		base_command.subscribe.subscription = subscription

		if sub_type == 1 then
			base_command.subscribe.subType = Pulsar::Proto::CommandSubscribe::SubType::Exclusive
		elsif sub_type == 2 then
			base_command.subscribe.subType = Pulsar::Proto::CommandSubscribe::SubType::Shared
		elsif sub_type == 3 then
			base_command.subscribe.subType = Pulsar::Proto::CommandSubscribe::SubType::Failover
		end	

		base_command.subscribe.consumer_id = @client_created_id
		base_command.subscribe.request_id = @request_id
	
	
		byte_cmd = base_command.serialize_to_string()
		total_length = byte_cmd.length + 4

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd

		@sock.write(total_frame)

		recv_length = @sock.read(4).unpack('N')[0]

		recv_cmd_length = @sock.read(4).unpack('N')[0]

		recv_cmd_byte = @sock.read(recv_cmd_length)
		
		recv_cmd = Pulsar::Proto::BaseCommand.new
		recv_cmd.parse_from_string(recv_cmd_byte)


		if recv_cmd.type == Pulsar::Proto::BaseCommand::Type::CONNECTED then
			return true
		elsif recv_cmd.type == Pulsar::Proto::BaseCommand::Type::SUCCESS then
			return true	
		else
			print("type:" + recv_cmd.type.to_s + "\n")
			return false
		end
	end

	def command_flow()
		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::FLOW
			)

		base_command.flow = Pulsar::Proto::CommandFlow.new
		base_command.flow.consumer_id = @client_created_id
		base_command.flow.messagePermits = 1000
	
		byte_cmd = base_command.serialize_to_string()
		total_length = byte_cmd.length + 4

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd

		@sock.write(total_frame)

	end

	def command_message()
		
		recv_length = @sock.read(4).unpack('N')[0]

		recv_cmd_length = @sock.read(4).unpack('N')[0]

		recv_cmd_byte = @sock.read(recv_cmd_length)
		
		recv_cmd = Pulsar::Proto::BaseCommand.new
		recv_cmd.parse_from_string(recv_cmd_byte)

		if recv_cmd.type == Pulsar::Proto::BaseCommand::Type::MESSAGE then

			consumer_id = recv_cmd.message.consumer_id
			ledgerId = recv_cmd.message.message_id.ledgerId
			entryId = recv_cmd.message.message_id.entryId
			
			recv_meta_length = 0
			recv_meta_byte = nil

			recv_magic = @sock.read(2)
			if recv_magic == [0x0e, 0x01].pack('C*') then
				recv_crc = @sock.read(4)
			
				recv_meta_length = @sock.read(4).unpack('N')[0]
				recv_meta_byte = @sock.read(recv_meta_length)
			else
				recv_remain = @sock.read(2)
				recv_meta_length = (recv_magic + recv_remain).unpack('N')[0]
				recv_meta_byte = @sock.read(recv_meta_length)
			end

			payload_length = recv_length - (4 + recv_cmd_length + 4 + recv_meta_length)
			
			message = @sock.read(payload_length)	

			recv_message = PulsarMessage.new
			recv_message.client_created_id = consumer_id
			recv_message.message_ledger_id = ledgerId
			recv_message.message_entry_id = entryId
			recv_message.message = message

			return recv_message
		end

	end


	def command_ack(consumer_id, ledgerId, entryId)
		base_command = Pulsar::Proto::BaseCommand.new(
				:type => Pulsar::Proto::BaseCommand::Type::ACK
			)

		base_command.ack = Pulsar::Proto::CommandAck.new
		base_command.ack.consumer_id = consumer_id
		base_command.ack.ack_type = Pulsar::Proto::CommandAck::AckType::Individual
		base_command.ack.message_id = [Pulsar::Proto::MessageIdData.new(
				:ledgerId => ledgerId,
				:entryId => entryId
		)]
		byte_cmd = base_command.serialize_to_string()
		total_length = byte_cmd.length + 4

		total_frame = [total_length].pack('N') + [byte_cmd.length].pack('N') + byte_cmd

		@sock.write(total_frame)

	end

	def connect(host, port)
		connect_socket(host, port)
		command_connect()
	end

	def close()
		close_socket()
	end

	def send(topic, message)
		s = command_producer(topic)

		# try again
		if s == false then
			command_lookup(topic)
			command_producer(topic)
		end
		command_send(message)
	end

	def subscribe(topic, subscription, sub_type)
		command_lookup(topic)
		command_subscribe(topic, subscription, sub_type)
		command_flow()
	end

	def get_message()
		m = command_message()
		return m	
	end

	def ack(consumer_id, ledgerId, entryId)
		command_ack(consumer_id, ledgerId, entryId)
	end

end

end

