Gem::Specification.new do |s|
  s.name        = 'rochefort'
  s.version     = '0.0.4'
  s.date        = '2018-02-15'
  s.summary     = "client for rochefort storage service"
  s.description = "rochefort is fast data append service that returns offsets to be indexed"
  s.authors     = ["Borislav Nikolov"]
  s.email       = 'jack@sofialondonmoskva.com'
  s.files       = ["lib/rochefort.rb"]
  s.homepage    = 'https://github.com/jackdoe/rochefort'
  s.license     = 'MIT'
  s.add_runtime_dependency('rest-client','~> 2.0')
  s.add_development_dependency('rake')
end
