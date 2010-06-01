require 'cassandra'

module ActiveCassandra
  class Base

    attr_reader :attributes

    def initialize(attributes={})
      self.attributes = attributes
    end

    module ConnectionManagement
      def establish_connection(*args)
        self.connection = Cassandra.new(*args)
      end
    end


    # CRUD operations

    def create
      
    end

    def read

    end

    def update

    end

    def delete

    end
    

  end
end