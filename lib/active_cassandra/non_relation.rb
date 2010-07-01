module ActiveCassandra
  class NonRelation
    
    def initialize(klass, column_family)
      @klass = klass
      @column_family = column_family
    end
    
    def attributes_from_columns(columns)
      attributes = {}
      columns.each do |column|
        attributes[column.name] = column.value
      end
      attributes
    end
    
    def all
      result = []
      @column_family.get(:all).each do |key, columns|
        next if columns.empty?
        result << @klass.instantiate(key, attributes_from_columns(columns))
      end
      result
    end
    
    def find(key)
      columns = @column_family.get(key)
      return nil if columns.blank?
      @klass.instantiate(key, attributes_from_columns(columns))
    end
    
    def insert(key, attributes)
      @column_family.insert(key, attributes)
    end
    
    def destroy(key)
      @column_family.remove(key)
    end
    
    alias :delete :destroy
    
    def first
      
    end
    
    def last
      
    end
    
    
  end
end

## TODO: rewrite all this code
## Needed at least add FinderMethods and to think about all others

require 'active_support/core_ext/object/blank'

module ActiveCassandraRewriting
  class NonRelation
    JoinOperation = Struct.new(:relation, :join_class, :on)
    ASSOCIATION_METHODS = [:includes, :eager_load, :preload]
    MULTI_VALUE_METHODS = [:select, :group, :order, :joins, :where, :having]
    SINGLE_VALUE_METHODS = [:limit, :offset, :lock, :readonly, :create_with, :from]

    #include FinderMethods, Calculations, SpawnMethods, QueryMethods, Batches

    delegate :length, :collect, :map, :each, :all?, :include?, :to => :to_a
    delegate :insert, :to => :arel

    attr_reader :column_family, :klass
    attr_accessor :extensions
    
    ## TODO: RAILS 3.0 COMPATABLE PART BEGIN
    alias :table :column_family
    ## TODO: RAILS 3.0 COMPATABLE PART END

    def initialize(klass, table, &block)
      @klass, @column_family = klass, @column_family

      @implicit_readonly = nil
      @loaded            = nil

      SINGLE_VALUE_METHODS.each {|v| instance_variable_set(:"@#{v}_value", nil)}
      (ASSOCIATION_METHODS + MULTI_VALUE_METHODS).each {|v| instance_variable_set(:"@#{v}_values", [])}
      @extensions = []

      apply_modules(Module.new(&block)) if block_given?
    end

    def new(*args, &block)
      with_create_scope { @klass.new(*args, &block) }
    end

    def initialize_copy(other)
      reset
    end

    alias build new

    def create(*args, &block)
      with_create_scope { @klass.create(*args, &block) }
    end

    def create!(*args, &block)
      with_create_scope { @klass.create!(*args, &block) }
    end

    def respond_to?(method, include_private = false)
      return true if arel.respond_to?(method, include_private) || Array.method_defined?(method) || @klass.respond_to?(method, include_private)

      if match = DynamicFinderMatch.match(method)
        return true if @klass.send(:all_attributes_exists?, match.attribute_names)
      elsif match = DynamicScopeMatch.match(method)
        return true if @klass.send(:all_attributes_exists?, match.attribute_names)
      else
        super
      end
    end

    def to_a
      return @records if loaded?

      @records = eager_loading? ? find_with_associations : @klass.find_by_sql(arel.to_sql)

      preload = @preload_values
      preload +=  @includes_values unless eager_loading?
      preload.each {|associations| @klass.send(:preload_associations, @records, associations) }

      # @readonly_value is true only if set explicity. @implicit_readonly is true if there are JOINS and no explicit SELECT.
      readonly = @readonly_value.nil? ? @implicit_readonly : @readonly_value
      @records.each { |record| record.readonly! } if readonly

      @loaded = true
      @records
    end
    
    ## TODO: IT'S TOO HARD TO COUNT ROWS IN CASSANDRA. NEED TO THINK ABOUT THIS
    def size
      loaded? ? @records.length : count
    end
    
    ## TODO: IT'S TOO HARD TO COUNT ROWS IN CASSANDRA. NEED TO THINK ABOUT THIS
    def empty?
      loaded? ? @records.empty? : count.zero?
    end

    def any?
      if block_given?
        to_a.any? { |*block_args| yield(*block_args) }
      else
        !empty?
      end
    end

    def many?
      if block_given?
        to_a.many? { |*block_args| yield(*block_args) }
      else
        @limit_value.present? ? to_a.many? : size > 1
      end
    end

    # Updates all records with details given if they match a set of conditions supplied, limits and order can
    # also be supplied. This method constructs a single SQL UPDATE statement and sends it straight to the
    # database. It does not instantiate the involved models and it does not trigger Active Record callbacks
    # or validations.
    #
    # ==== Parameters
    #
    # * +updates+ - A string, array, or hash representing the SET part of an SQL statement.
    # * +conditions+ - A string, array, or hash representing the WHERE part of an SQL statement. See conditions in the intro.
    # * +options+ - Additional options are <tt>:limit</tt> and <tt>:order</tt>, see the examples for usage.
    #
    # ==== Examples
    #
    #   # Update all customers with the given attributes
    #   Customer.update_all :wants_email => true
    #
    #   # Update all books with 'Rails' in their title
    #   Book.update_all "author = 'David'", "title LIKE '%Rails%'"
    #
    #   # Update all avatars migrated more than a week ago
    #   Avatar.update_all ['migrated_at = ?', Time.now.utc], ['migrated_at > ?', 1.week.ago]
    #
    #   # Update all books that match our conditions, but limit it to 5 ordered by date
    #   Book.update_all "author = 'David'", "title LIKE '%Rails%'", :order => 'created_at', :limit => 5
    
    ## TODO: REALLY HARD METHOD TO REWRITE. NEED A LOT OF THINK ABOUT IT
    def update_all(updates, conditions = nil, options = {})
      if conditions || options.present?
        where(conditions).apply_finder_options(options.slice(:limit, :order)).update_all(updates)
      else
        # Apply limit and order only if they're both present
        if @limit_value.present? == @order_values.present?
          arel.update(@klass.send(:sanitize_sql_for_assignment, updates))
        else
          except(:limit, :order).update_all(updates)
        end
      end
    end

    # Updates an object (or multiple objects) and saves it to the database, if validations pass.
    # The resulting object is returned whether the object was saved successfully to the database or not.
    #
    # ==== Parameters
    #
    # * +id+ - This should be the id or an array of ids to be updated.
    # * +attributes+ - This should be a hash of attributes to be set on the object, or an array of hashes.
    #
    # ==== Examples
    #
    #   # Updating one record:
    #   Person.update(15, :user_name => 'Samuel', :group => 'expert')
    #
    #   # Updating multiple records:
    #   people = { 1 => { "first_name" => "David" }, 2 => { "first_name" => "Jeremy" } }
    #   Person.update(people.keys, people.values)
    def update(id, attributes)
      if id.is_a?(Array)
        idx = -1
        id.collect { |one_id| idx += 1; update(one_id, attributes[idx]) }
      else
        object = find(id)
        object.update_attributes(attributes)
        object
      end
    end

    # Destroys the records matching +conditions+ by instantiating each
    # record and calling its +destroy+ method. Each object's callbacks are
    # executed (including <tt>:dependent</tt> association options and
    # +before_destroy+/+after_destroy+ Observer methods). Returns the
    # collection of objects that were destroyed; each will be frozen, to
    # reflect that no changes should be made (since they can't be
    # persisted).
    #
    # Note: Instantiation, callback execution, and deletion of each
    # record can be time consuming when you're removing many records at
    # once. It generates at least one SQL +DELETE+ query per record (or
    # possibly more, to enforce your callbacks). If you want to delete many
    # rows quickly, without concern for their associations or callbacks, use
    # +delete_all+ instead.
    #
    # ==== Parameters
    #
    # * +conditions+ - A string, array, or hash that specifies which records
    #   to destroy. If omitted, all records are destroyed. See the
    #   Conditions section in the introduction to ActiveRecord::Base for
    #   more information.
    #
    # ==== Examples
    #
    #   Person.destroy_all("last_login < '2004-04-04'")
    #   Person.destroy_all(:status => "inactive")
    
    ## TODO: REALLY HARD METHOD TO REWRITE. NEED A LOT OF THINK ABOUT IT
    def destroy_all(conditions = nil)
      if conditions
        where(conditions).destroy_all
      else
        to_a.each {|object| object.destroy}
        reset
      end
    end

    # Destroy an object (or multiple objects) that has the given id, the object is instantiated first,
    # therefore all callbacks and filters are fired off before the object is deleted.  This method is
    # less efficient than ActiveRecord#delete but allows cleanup methods and other actions to be run.
    #
    # This essentially finds the object (or multiple objects) with the given id, creates a new object
    # from the attributes, and then calls destroy on it.
    #
    # ==== Parameters
    #
    # * +id+ - Can be either an Integer or an Array of Integers.
    #
    # ==== Examples
    #
    #   # Destroy a single object
    #   Todo.destroy(1)
    #
    #   # Destroy multiple objects
    #   todos = [1,2,3]
    #   Todo.destroy(todos)
    def destroy(id)
      if id.is_a?(Array)
        id.map { |one_id| destroy(one_id) }
      else
        find(id).destroy
      end
    end

    # Deletes the records matching +conditions+ without instantiating the records first, and hence not
    # calling the +destroy+ method nor invoking callbacks. This is a single SQL DELETE statement that
    # goes straight to the database, much more efficient than +destroy_all+. Be careful with relations
    # though, in particular <tt>:dependent</tt> rules defined on associations are not honored.  Returns
    # the number of rows affected.
    #
    # ==== Parameters
    #
    # * +conditions+ - Conditions are specified the same way as with +find+ method.
    #
    # ==== Example
    #
    #   Post.delete_all("person_id = 5 AND (category = 'Something' OR category = 'Else')")
    #   Post.delete_all(["person_id = ? AND (category = ? OR category = ?)", 5, 'Something', 'Else'])
    #
    # Both calls delete the affected posts all at once with a single DELETE statement. If you need to destroy dependent
    # associations or call your <tt>before_*</tt> or +after_destroy+ callbacks, use the +destroy_all+ method instead.
    
    ## TODO: REALLY HARD METHOD TO REWRITE. NEED A LOT OF THINK ABOUT IT
    def delete_all(conditions = nil)
      conditions ? where(conditions).delete_all : arel.delete.tap { reset }
    end

    # Deletes the row with a primary key matching the +id+ argument, using a
    # SQL +DELETE+ statement, and returns the number of rows deleted. Active
    # Record objects are not instantiated, so the object's callbacks are not
    # executed, including any <tt>:dependent</tt> association options or
    # Observer methods.
    #
    # You can delete multiple rows at once by passing an Array of <tt>id</tt>s.
    #
    # Note: Although it is often much faster than the alternative,
    # <tt>#destroy</tt>, skipping callbacks might bypass business logic in
    # your application that ensures referential integrity or performs other
    # essential jobs.
    #
    # ==== Examples
    #
    #   # Delete a single row
    #   Todo.delete(1)
    #
    #   # Delete multiple rows
    #   Todo.delete([2,3,4])
    
    ## TODO: REALLY HARD METHOD TO REWRITE. NEED A LOT OF THINK ABOUT IT
    def delete(id_or_array)
      where(@klass.primary_key => id_or_array).delete_all
    end

    def loaded?
      @loaded
    end

    def reload
      reset
      to_a # force reload
      self
    end

    def reset
      @first = @last = @to_sql = @order_clause = @scope_for_create = @arel = @loaded = nil
      @should_eager_load = @join_dependency = nil
      @records = []
      self
    end

    def primary_key
      @primary_key ||= table[@klass.primary_key]
    end

##  TODO: SURELY WILL BE DEPRECATED. NEED TO FIND ALL USES AND REWRITE THEM
#    def to_sql
#      @to_sql ||= arel.to_sql
#    end

##  TODO: SEEMS NICE. BUT Arel::Predicates::Equality CONFUSING. 
    def scope_for_create
      @scope_for_create ||= begin
        @create_with_value || @where_values.inject({}) do |hash, where|
          if where.is_a?(Arel::Predicates::Equality)
            hash[where.operand1.name] = where.operand2.respond_to?(:value) ? where.operand2.value : where.operand2
          end

          hash
        end
      end
    end

    def eager_loading?
      @should_eager_load ||= (@eager_load_values.any? || (@includes_values.any? && references_eager_loaded_tables?))
    end

    def ==(other)
      case other
      when Relation
        other.to_sql == to_sql
      when Array
        to_a == other.to_a
      end
    end

    def inspect
      to_a.inspect
    end

    protected

##  TODO: NEED A LOT OF THINK ABOUT IT
    def method_missing(method, *args, &block)
      if Array.method_defined?(method)
        to_a.send(method, *args, &block)
      elsif @klass.scopes[method]
        merge(@klass.send(method, *args, &block))
      elsif @klass.respond_to?(method)
        @klass.send(:with_scope, self) { @klass.send(method, *args, &block) }
      elsif arel.respond_to?(method)
        arel.send(method, *args, &block)
      elsif match = DynamicFinderMatch.match(method)
        attributes = match.attribute_names
        super unless @klass.send(:all_attributes_exists?, attributes)

        if match.finder?
          find_by_attributes(match, attributes, *args)
        elsif match.instantiator?
          find_or_instantiator_by_attributes(match, attributes, *args, &block)
        end
      else
        super
      end
    end

    private

    def with_create_scope
      @klass.send(:with_scope, :create => scope_for_create, :find => {}) { yield }
    end

    def references_eager_loaded_tables?
      joined_tables = (tables_in_string(arel.joins(arel)) + [table.name, table.table_alias]).compact.uniq
      (tables_in_string(to_sql) - joined_tables).any?
    end

    def tables_in_string(string)
      return [] if string.blank?
      string.scan(/([a-zA-Z_][\.\w]+).?\./).flatten.uniq
    end

  end
end


