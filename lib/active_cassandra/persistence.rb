module ActiveCassandra
  module Persistence

    def self.included(base)
      base.class_eval do
        extend  ActiveCassandra::Persistence::ClassMethods
        include ActiveCassandra::Persistence::InstanceMethods
      end
    end

    module ClassMethods

      # CRUD operations

      def create(attributes)
        object = instantiate(next_key, attributes)
        object.save
        object
      end

      def read(key)
        columns = keyspace[key].get(column_family, nil)
        object = nil
        unless columns.empty?
          attributes = {}
          columns.each do |column|
            attributes[column.name] = column.value
          end
          object = instantiate(key, attributes)
        end
        object
      end

      def update(key, attributes)
        keyspace[key].insert(column_family, nil, attributes, Time.now)
      end

      def delete(key)
        keyspace[key].remove(column_family, nil, nil, Time.now)
      end

      def column_family
        name.pluralize
      end

      private

      def next_key
        UUIDTools::UUID.random_create.to_s
      end

      def instantiate(key, attributes)
        object = self.allocate
        object.key = key
        object.attributes = attributes
        object
      end

    end

    module InstanceMethods
      def save
        self.class.keyspace[self.key].insert(self.class.column_family, nil, self.attributes, Time.now)
      end

    end

  end
end