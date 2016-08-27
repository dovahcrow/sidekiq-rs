require 'sidekiq'

class Printer
  include Sidekiq::Worker
  sidekiq_options :queue => :analytics, :backtrace => true
  def perform(name)
    puts "Hello, #{name}!"
  end
end
