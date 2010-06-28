require 'active_support/core_ext/array/wrap'

module ActiveCassandra
  
  module Callbacks
    extend ActiveSupport::Concern

    CALLBACKS = [
      :after_initialize, :after_find, :before_validation, :after_validation,
      :before_save, :around_save, :after_save, :before_create, :around_create,
      :after_create, :before_update, :around_update, :after_update,
      :before_destroy, :around_destroy, :after_destroy
    ]

    included do
      extend ActiveModel::Callbacks

      define_callbacks :validation, :terminator => "result == false", :scope => [:kind, :name]

      define_model_callbacks :initialize, :find, :only => :after
      define_model_callbacks :save, :create, :update, :destroy
    end

    module ClassMethods
      def method_added(meth)
        super
        if CALLBACKS.include?(meth.to_sym)
          ActiveSupport::Deprecation.warn("Base##{meth} has been deprecated, please use Base.#{meth} :method instead", caller[0,1])
          send(meth.to_sym, meth.to_sym)
        end
      end

      def before_validation(*args, &block)
        options = args.last
        if options.is_a?(Hash) && options[:on]
          options[:if] = Array.wrap(options[:if])
          options[:if] << "@_on_validate == :#{options[:on]}"
        end
        set_callback(:validation, :before, *args, &block)
      end

      def after_validation(*args, &block)
        options = args.extract_options!
        options[:prepend] = true
        options[:if] = Array.wrap(options[:if])
        options[:if] << "!halted && value != false"
        options[:if] << "@_on_validate == :#{options[:on]}" if options[:on]
        set_callback(:validation, :after, *(args << options), &block)
      end
    end

    def valid?(*) #:nodoc:
      @_on_validate = new_record? ? :create : :update
      _run_validation_callbacks { super }
    end

    def destroy #:nodoc:
      _run_destroy_callbacks { super }
    end

    def deprecated_callback_method(symbol) #:nodoc:
      if respond_to?(symbol, true)
        ActiveSupport::Deprecation.warn("Overwriting #{symbol} in your models has been deprecated, please use Base##{symbol} :method_name instead")
        send(symbol)
      end
    end

  private
    def create_or_update #:nodoc:
      _run_save_callbacks { super }
    end

    def create #:nodoc:
      _run_create_callbacks { super }
    end

    def update(*) #:nodoc:
      _run_update_callbacks { super }
    end
  end
end
