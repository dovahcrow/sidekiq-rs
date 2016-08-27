require 'sidekiq'

class Error
  include Sidekiq::Worker
  sidekiq_options :queue => :analytics, :backtrace => true, :retry => 5

  def perform()
  end
end
