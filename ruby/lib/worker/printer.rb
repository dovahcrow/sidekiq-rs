require 'sidekiq'

class Printer
  include Sidekiq::Worker

  def perform(name)
    puts "Hello, #{name}!"
  end
end
