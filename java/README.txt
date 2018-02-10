java client rochefort

install:

  cd client && mvn install

example usage:

  import nl.prymr.rochefort.Client;


  Client client = new Client("http://localhost:8000");

  // stores data for "exampleKey"
  long offset = client.append("exampleKey", new byte[]{0,1,2,3,4,5});

  // fetches data for "exampleKey" with the appropriate offset
  byte[] data = client.get("exampleKey",offset);
  // data is now new byte[]{0,1,2,3,4,5}

enjoy

dependencies:

  <dependency>
    <groupId>com.mashape.unirest</groupId>
    <artifactId>unirest-java</artifactId>
    <version>1.4.9</version>
  </dependency>

Unirest is quite cool, check it out on: http://unirest.io/java.html
