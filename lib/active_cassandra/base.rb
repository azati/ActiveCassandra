module ActiveCassandra
  class Base
    #include AttributeMethods
    
    cattr_accessor :configurations, :instance_writer => false
    @@configurations = {}
    
    class_inheritable_accessor :connection

    cattr_accessor :primary_key_prefix_type, :instance_writer => false
    @@primary_key_prefix_type = nil
    
    class << self
     
      delegate :all, :first, :last, :to => :getter
      delegate :find, :to => :getter
      delegate :destroy, :to => :getter
      
      def establish_connection(spec)
        config = configurations[spec.to_s].symbolize_keys
        host     = (config[:host] || 'localhost') 
        port     = (config[:port] || '9160')
        username = config[:username] ? config[:username].to_s : 'root'
        password = config[:password].to_s
        keyspace = config[:keyspace]
  
        # Require the MySQL driver and define Mysql::Result.all_hashes
        unless defined? CassandraRuby
          begin
            require_library_or_gem 'cassandra_ruby'
          rescue LoadError
            $stderr.puts '!!! Please install the cassandra gem and try again: gem install cassandra.'
            raise
          end
        end

      
        self.connection = CassandraRuby::Keyspace.new(CassandraRuby::Cassandra.new(host), keyspace)
      end
     
      def instantiate(key, row)
        object = self.allocate
  
        object.instance_variable_set(:'@attributes', row)
        object.instance_variable_set(:'@attributes_cache', {})
        object.instance_variable_set(:@new_record, false)
        object.instance_variable_set(:@readonly, false)
        object.instance_variable_set(:@destroyed, false)
        object.instance_variable_set(:@marked_for_destruction, false)
        object.instance_variable_set(:@previously_changed, {})
        object.instance_variable_set(:@changed_attributes, {})
        object.instance_variable_set(:@key, key)
  
        object.send(:_run_find_callbacks)
        object.send(:_run_initialize_callbacks)
  
        object
      end
      
      def find_sti_class(type_name)
        if type_name.blank? || !columns_hash.include?(inheritance_column)
          self
        else
          begin
            compute_type(type_name)
          rescue NameError
            raise SubclassNotFound,
              "The single-table inheritance mechanism failed to locate the subclass: '#{type_name}'. " +
              "This error is raised because the column '#{inheritance_column}' is reserved for storing the class in case of inheritance. " +
              "Please rename this column if you didn't intend it to be used for storing the inheritance class " +
              "or overwrite #{name}.inheritance_column to use another column for that information."
          end
        end
      end
      
      def getter
        @getter ||= NonRelation.new(self, column_family)
      end
      
      def indexer
        @indexer ||= Indexer.new(self, indexes_column_family)
      end
      
      def indexes_column_family
        SuperColumnFamily.new("indexes", connection)
      end
      
      def column_family
        ColumnFamily.new(column_family_name, connection)
      end
      
      def column_family_name
        self.name.demodulize.underscore.pluralize
      end
     
      def create(attributes = nil, &block)
        if attributes.is_a?(Array)
          attributes.collect { |attr| create(attr, &block) }
        else
          object = new(attributes)
          yield(object) if block_given?
          object.save
          object
        end
      end

      def validates_uniqueness_of(*attr_names)
        #validates_with UniquenessValidator, _merge_attributes(attr_names)
      end

      def base_class
        class_of_active_record_descendant(self)
      end

      def class_of_active_record_descendant(klass)
        if klass.superclass == Base || klass.superclass.abstract_class?
          klass
        elsif klass.superclass.nil?
          raise ActiveCassandraError, "#{name} doesn't belong in a hierarchy descending from ActiveRecord"
        else
          class_of_active_record_descendant(klass.superclass)
        end
      end
      
      def method_missing(method_id, *arguments, &block)
 
        if match = DynamicFinderMatch.match(method_id)
          attribute_names = match.attribute_names
          super unless all_attributes_exists?(attribute_names)
          if match.finder?
            indexer.find(attribute_names[0], arguments[0])
#            options = arguments.extract_options!
#            relation = options.any? ? construct_finder_arel(options, current_scoped_methods) : scoped
#            relation.send :find_by_attributes, match, attribute_names, *arguments
          elsif match.instantiator?
#            scoped.send :find_or_instantiator_by_attributes, match, attribute_names, *arguments, &block
          end
#          elsif match = DynamicScopeMatch.match(method_id)
#            attribute_names = match.attribute_names
#            super unless all_attributes_exists?(attribute_names)
#            if match.scope?
#              self.class_eval <<-METHOD, __FILE__, __LINE__ + 1
#                def self.#{method_id}(*args)                        # def self.scoped_by_user_name_and_password(*args)
#                  options = args.extract_options!                   #   options = args.extract_options!
#                  attributes = construct_attributes_from_arguments( #   attributes = construct_attributes_from_arguments(
#                    [:#{attribute_names.join(',:')}], args          #     [:user_name, :password], args
#                  )                                                 #   )
#                                                                    #
#                  scoped(:conditions => attributes)                 #   scoped(:conditions => attributes)
#                end                                                 # end
#              METHOD
#              send(method_id, *arguments)
#            end
        else
          super
        end
        
      end
    
      def all_attributes_exists?(attribute_names)
        #attribute_names = expand_attribute_names_for_aggregates(attribute_names)
        attribute_names.all? { |name| column_methods_hash.include?(name.to_sym) }
      end
    
      def with_scope(method_scoping = {}, action = :merge, &block)
        p method_scoping.inspect
        result = []
        method_scoping[:find].each do |key, value|
          result << indexer.find(key.to_s, value)[0]        
        end
        
          yield
        
        
        #result[0]
      end
    
      alias :find_by_key :find
    
   end
   
   def initialize(attributes = nil, &block)
     @attributes = {}
     
     self.class.columns.each do |column|
       @attributes[column.name] = column.value
     end
     
     @attributes_cache = {}
     @new_record = true
     @readonly = false
     @destroyed = false
     @marked_for_destruction = false
     @previously_changed = {}
     @changed_attributes = {}
     ## USE GUID HERE
     @key = next_key
     
     self.attributes = attributes unless attributes.nil?

     result = yield self if block_given?
     _run_initialize_callbacks
     result
     
   end
   
   def attributes=(new_attributes, guard_protected_attributes = true)
     return if new_attributes.nil?
     
     attributes = new_attributes.stringify_keys 

     #multi_parameter_attributes = []
     #attributes = remove_attributes_protected_from_mass_assignment(attributes) if guard_protected_attributes

     attributes.each do |k, v|
       if k.include?("(")
         multi_parameter_attributes << [ k, v ]
       else
         respond_to?(:"#{k}=") ? send(:"#{k}=", v) : raise(UnknownAttributeError, "unknown attribute: #{k}")
       end
     end

     #assign_multiparameter_attributes(multi_parameter_attributes)
   end

  # Returns a hash of all the attributes with their names as keys and the values of the attributes as values.
   def attributes
     attrs = {}
     column_names.each { |name| attrs[name] = read_attribute(name) }
     attrs
   end
   
   def columns
     #unless defined?(@columns) && @columns
     #  @columns = connection.columns(table_name, "#{name} Columns")
     #  @columns.each { |column| column.primary = column.name == primary_key }
     #end
     self.class.columns
   end

   def column_names
     self.class.column_names
   end
   
   def columns_hash
     @columns_hash ||= columns.inject({}) { |hash, column| hash[column.name] = column; hash }
   end
   

    
   def column_for_attribute(name)
     self.class.columns_hash[name.to_s]
   end
   
   def next_key
     rand(100000).to_s
   end
   
   def key
     @key
   end

   alias :id :key
   
   protected 
   
    def clone_attribute_value(reader_method, attribute_name)
      value = send(reader_method, attribute_name)
      value.duplicable? ? value.clone : value
    rescue TypeError, NoMethodError
      value
    end
    
  end
  
  Base.class_eval do
  
   
    include ActiveCassandra::Persistence
    extend ActiveModel::Naming
    extend ActiveSupport::Benchmarkable

    include Validations
    
    include ActiveModel::AttributeMethods
    include ActiveModel::Validations
    include ActiveModel::Conversion
    
    include AttributeMethods
    
    include AttributeMethods::Read, AttributeMethods::Write, AttributeMethods::PrimaryKey, AttributeMethods::Dirty

    include Callbacks
    
    
    include Columns

  end
  
end

ActiveSupport.run_load_hooks(:active_cassandra, ActiveCassandra::Base)


