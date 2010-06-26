module ActiveCassandra
  module Columns

    def self.included(base)
      base.extend ClassMethods
    end

    class Column

      attr_reader   :name
      attr_accessor :value
      attr_reader   :timestamp

      def initialize(name, value, timestamp = Time.now.to_i)
        @name = name
        @value = value
        @timestamp = timestamp
      end

    end

    def attributes

    end

    module ClassMethods
      

      def column(name, type, options = {})
        @columns ||= []
        value = options[:default].blank? ? type.new : options[:default]
        @columns << Column.new(name.to_s, value)
      end

      def columns
        @columns
      end

      def column_names
        @columns.inject([]) {|result, column| result << column.name}
      end

    end
  end

end