## TODO: review all this code
module ActiveCassandra
  module Persistence
    # Returns true if this object hasn't been saved yet -- that is, a record for the object doesn't exist yet; otherwise, returns false.
    def new_record?
      @new_record
    end

    # Returns true if this object has been destroyed, otherwise returns false.
    def destroyed?
      @destroyed
    end

    # Returns if the record is persisted, i.e. it's not a new record and it was not destroyed.
    def persisted?
      !(new_record? || destroyed?)
    end

    # :call-seq:
    #   save(options)
    #
    # Saves the model.
    #
    # If the model is new a record gets created in the database, otherwise
    # the existing record gets updated.
    #
    # By default, save always run validations. If any of them fail the action
    # is cancelled and +save+ returns +false+. However, if you supply
    # :validate => false, validations are bypassed altogether. See
    # ActiveRecord::Validations for more information.
    #
    # There's a series of callbacks associated with +save+. If any of the
    # <tt>before_*</tt> callbacks return +false+ the action is cancelled and
    # +save+ returns +false+. See ActiveRecord::Callbacks for further
    # details.
    def save(*)
      create_or_update
    end

    # Saves the model.
    #
    # If the model is new a record gets created in the database, otherwise
    # the existing record gets updated.
    #
    # With <tt>save!</tt> validations always run. If any of them fail
    # ActiveRecord::RecordInvalid gets raised. See ActiveRecord::Validations
    # for more information.
    #
    # There's a series of callbacks associated with <tt>save!</tt>. If any of
    # the <tt>before_*</tt> callbacks return +false+ the action is cancelled
    # and <tt>save!</tt> raises ActiveRecord::RecordNotSaved. See
    # ActiveRecord::Callbacks for further details.
    def save!(*)
      create_or_update || raise(RecordNotSaved)
    end

    # Deletes the record in the database and freezes this instance to
    # reflect that no changes should be made (since they can't be
    # persisted). Returns the frozen instance.
    #
    # The row is simply removed with a SQL +DELETE+ statement on the
    # record's primary key, and no callbacks are executed.
    #
    # To enforce the object's +before_destroy+ and +after_destroy+
    # callbacks, Observer methods, or any <tt>:dependent</tt> association
    # options, use <tt>#destroy</tt>.
    def delete
      self.class.delete(id) if persisted?
      @destroyed = true
      freeze
    end

    # Deletes the record in the database and freezes this instance to reflect that no changes should
    # be made (since they can't be persisted).
    ## TODO: WHAT TO DO WITH UNSCOPED ?? 
    def destroy
      ## TODO: REVIEW LATER
      #if persisted?
      #  self.class.unscoped.where(self.class.arel_table[self.class.primary_key].eq(id)).delete_all
      #end
      
      self.class.delete(id) if persisted?

      @destroyed = true
      freeze
    end

    # Returns an instance of the specified +klass+ with the attributes of the current record. This is mostly useful in relation to
    # single-table inheritance structures where you want a subclass to appear as the superclass. This can be used along with record
    # identification in Action Pack to allow, say, <tt>Client < Company</tt> to do something like render <tt>:partial => @client.becomes(Company)</tt>
    # to render that instance using the companies/company partial instead of clients/client.
    #
    # Note: The new instance will share a link to the same attributes as the original class. So any change to the attributes in either
    # instance will affect the other.
    def becomes(klass)
      became = klass.new
      became.instance_variable_set("@attributes", @attributes)
      became.instance_variable_set("@attributes_cache", @attributes_cache)
      became.instance_variable_set("@new_record", new_record?)
      became.instance_variable_set("@destroyed", destroyed?)
      became
    end

    # Updates a single attribute and saves the record without going through the normal validation procedure.
    # This is especially useful for boolean flags on existing records. The regular +update_attribute+ method
    # in Base is replaced with this when the validations module is mixed in, which it is by default.
    def update_attribute(name, value)
      send("#{name}=", value)
      save(:validate => false)
    end

    # Updates all the attributes from the passed-in Hash and saves the record. If the object is invalid, the saving will
    # fail and false will be returned.
    def update_attributes(attributes)
      self.attributes = attributes
      save
    end

    # Updates an object just like Base.update_attributes but calls save! instead of save so an exception is raised if the record is invalid.
    def update_attributes!(attributes)
      self.attributes = attributes
      save!
    end

    # Initializes +attribute+ to zero if +nil+ and adds the value passed as +by+ (default is 1).
    # The increment is performed directly on the underlying attribute, no setter is invoked.
    # Only makes sense for number-based attributes. Returns +self+.
    def increment(attribute, by = 1)
      self[attribute] ||= 0
      self[attribute] += by
      self
    end

    # Wrapper around +increment+ that saves the record. This method differs from
    # its non-bang version in that it passes through the attribute setter.
    # Saving is not subjected to validation checks. Returns +true+ if the
    # record could be saved.
    def increment!(attribute, by = 1)
      increment(attribute, by).update_attribute(attribute, self[attribute])
    end

    # Initializes +attribute+ to zero if +nil+ and subtracts the value passed as +by+ (default is 1).
    # The decrement is performed directly on the underlying attribute, no setter is invoked.
    # Only makes sense for number-based attributes. Returns +self+.
    def decrement(attribute, by = 1)
      self[attribute] ||= 0
      self[attribute] -= by
      self
    end

    # Wrapper around +decrement+ that saves the record. This method differs from
    # its non-bang version in that it passes through the attribute setter.
    # Saving is not subjected to validation checks. Returns +true+ if the
    # record could be saved.
    def decrement!(attribute, by = 1)
      decrement(attribute, by).update_attribute(attribute, self[attribute])
    end

    # Assigns to +attribute+ the boolean opposite of <tt>attribute?</tt>. So
    # if the predicate returns +true+ the attribute will become +false+. This
    # method toggles directly the underlying value without calling any setter.
    # Returns +self+.
    def toggle(attribute)
      self[attribute] = !send("#{attribute}?")
      self
    end

    # Wrapper around +toggle+ that saves the record. This method differs from
    # its non-bang version in that it passes through the attribute setter.
    # Saving is not subjected to validation checks. Returns +true+ if the
    # record could be saved.
    def toggle!(attribute)
      toggle(attribute).update_attribute(attribute, self[attribute])
    end

    # Reloads the attributes of this object from the database.
    # The optional options argument is passed to find when reloading so you
    # may do e.g. record.reload(:lock => true) to reload the same record with
    # an exclusive row lock.
    def reload(options = nil)
      ## TODO: NEED REVIEW LATER
      #clear_aggregation_cache
      #clear_association_cache
      #@attributes.update(self.class.send(:with_exclusive_scope) { self.class.find(self.id, options) }.instance_variable_get('@attributes'))
      @attributes.update(self.class.unscoped.get(self.key))
      @attributes_cache = {}
      self
    end

  private
    def create_or_update
      raise ReadOnlyRecord if readonly?
      result = new_record? ? create : update
      result != false
    end

    # Updates the associated record with values matching those of the instance attributes.
    # Returns the number of affected rows.
    def update(attribute_names = @attributes.keys)
      ## TODO: NEED REVIEW LATER
      #attributes_with_values = arel_attributes_values(false, false, attribute_names)
      #return 0 if attributes_with_values.empty?
      #self.class.unscoped.where(self.class.arel_table[self.class.primary_key].eq(id)).arel.update(attributes_with_values)
      
      attributes_with_values = attributes_values(false, false, attribute_names)
      return 0 if attributes_with_values.empty?
      self.class.unscoped.insert(key, attributes_with_values)
    end

    # Creates a record with values matching those of the instance attributes
    # and returns its id.
    def create
      ## TODO: NEED REVIEW LATER
      #if self.id.nil? && connection.prefetch_primary_key?(self.class.table_name)
      #  self.id = connection.next_sequence_value(self.class.sequence_name)
      #end
      #attributes_values = arel_attributes_values
      
      #new_id = if attributes_values.empty?
      #  self.class.unscoped.insert connection.empty_insert_statement_value
      #else
      #  self.class.unscoped.insert attributes_values
      #end

      #self.id ||= new_id

      attributes_with_values = attributes_values(false, false, attribute_names)

      self.class.unscoped.insert(key, attributes_with_values)

      @new_record = false
      key
    end

    # Initializes the attributes array with keys matching the columns from the linked table and
    # the values matching the corresponding default value of that column, so
    # that a new instance, or one populated from a passed-in Hash, still has all the attributes
    # that instances loaded from the database would.
    def attributes_from_column_definition
      self.class.columns.inject({}) do |attributes, column|
        attributes[column.name] = column.default unless column.name == self.class.primary_key
        attributes
      end
    end
  end
end