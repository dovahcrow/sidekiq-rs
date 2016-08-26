require 'sidekiq'

class Dummy
  include Sidekiq::Worker
  sidekiq_options :queue => :analytics
  
  def perform(name)
    puts "Hello, #{name}!"
  end
end
