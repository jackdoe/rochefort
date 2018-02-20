export ROCHEFORT_TEST=http://localhost:8002
cd clients && cd java && mvn test && cd ../ruby && rake test && cd ../js/ && npm test
