module ActiveCassandra
  module Columns

    def self.included(base)
      base.extend ClassMethods
    end

    class Column

      attr_reader   :name      
      attr_reader   :type
      attr_reader   :index
      attr_reader   :primary
      attr_reader   :default
      
      def initialize(name, default, type, index = false, null = true)
        @name, @type, @index, @null = name, type, index, null
        #@limit, @precision, @scale = extract_limit(sql_type), extract_precision(sql_type), extract_scale(sql_type)
        #@type = simplified_type(sql_type)
        @default = extract_default(default)
        
        @primary = nil
      end
      
      #def initialize(name, type, default, options = {})
      #  @name = name
      #  @default = default
      #  @type = type
      #  @index = options[:index] == true
      #  @primary = nil
      #  #@timestamp = timestamp
      #end
      
      def number?
        false
      end
      
      def type_cast(value)
        #return "" if value.nil?
        
        return nil if value.nil?
        case type
          when :string    then value
          when :text      then value
          when :integer   then value.to_i rescue value ? 1 : 0
          when :float     then value.to_f
          when :decimal   then self.class.value_to_decimal(value)
          when :datetime  then self.class.string_to_time(value)
          when :timestamp then self.class.string_to_time(value)
          when :time      then self.class.string_to_dummy_time(value)
          when :date      then self.class.string_to_date(value)
          when :binary    then self.class.binary_to_string(value)
          when :boolean   then self.class.value_to_boolean(value)
          else value
        end
      end
      
      def extract_default(default)
        type_cast(default)
      end

    end

    def attributes

    end

    module ClassMethods
      

      def column(name, type, options = {})
        @columns ||= []     
        @columns << Column.new(name.to_s, options[:default], type, options[:index], false)
      end

      def columns
        @columns
      end

      def column_names
        @columns.inject([]) {|result, column| result << column.name}
      end
      
      def columns_hash
        @columns_hash ||= columns.inject({}) { |hash, column| hash[column.name] = column; hash }
      end
      
      def column_methods_hash #:nodoc:
        @dynamic_methods_hash ||= column_names.inject(Hash.new(false)) do |methods, attr|
          attr_name = attr.to_s
          methods[attr.to_sym]       = attr_name
          methods["#{attr}=".to_sym] = attr_name
          methods["#{attr}?".to_sym] = attr_name
          methods["#{attr}_before_type_cast".to_sym] = attr_name
          methods
        end
      end

    end
  end

end