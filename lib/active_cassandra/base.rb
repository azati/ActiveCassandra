require 'cassandra_ruby'

require 'active_cassandra/persistence'

module ActiveCassandra
  class Base

    attr_accessor :attributes
    attr_accessor :key
    cattr_accessor :configurations
    cattr_accessor :keyspace

    module ConnectionManagement
      def establish_connection(spec)
        configuration = configurations[spec.to_s]
        host = configuration['host']
        port = configuration['port']
        keyspace = configuration['keyspace']
        cassandra = CassandraRuby::Cassandra.new(host, {:port => port})
        cassandra.connect
        self.keyspace = CassandraRuby::Keyspace.new(cassandra, keyspace)
      end
    end
    extend ConnectionManagement

    include Persistence

    def initialize(attributes={})
      self.attributes = attributes
    end

  end
end