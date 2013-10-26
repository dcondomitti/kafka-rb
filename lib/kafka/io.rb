# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  module IO
    attr_accessor :host, :port, :compression, :ssl, :ssl_socket, :tcp_socket

    HOST = "localhost"
    PORT = 9092

    def connect(host, port, ssl = false)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.ssl = ssl
      open_socket
    end

    def open_socket
      self.tcp_socket = TCPSocket.new(host, port)
      ssl! if ssl?
    end

    def reconnect
      open_socket
    rescue
      self.disconnect
      raise
    end

    def disconnect
      self.tcp_socket.close rescue nil
      self.socket = nil
    end

    def read(length)
      self.socket.read(length) || raise(SocketError, "no data")
    rescue
      self.disconnect
      raise SocketError, "cannot read: #{$!.message}"
    end

    def write(data)
      self.reconnect unless self.socket
      self.socket.write(data)
    rescue
      self.disconnect
      raise SocketError, "cannot write: #{$!.message}"
    end

    def socket
      return self.tcp_socket unless ssl?
      self.ssl_socket
    end

    def ssl!
      self.ssl_socket = OpenSSL::SSL::SSLSocket.new(self.tcp_socket)
      self.ssl_socket.sync_close = true
      self.ssl_socket.connect
    end

    def ssl?
      !!self.ssl
    end
  end
end
