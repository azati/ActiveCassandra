require "active_cassandra"

module ActiveCassandra
  class Railtie < Rails::Railtie
    
    initializer "active_cassandra.initialize_database" do |app|
      ActiveSupport.on_load(:active_cassandra) do
        self.configurations = app.config.database_configuration      
        ActiveCassandra::Base::establish_connection(Rails.env)
      end
    end
    
    
  end
end