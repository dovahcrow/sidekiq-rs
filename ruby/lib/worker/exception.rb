require 'sidekiq'

class Excep
  include Sidekiq::Worker
  sidekiq_options :queue => :analytics

  def perform(name)
    p 1
    raise name
    p 2
  end
end
