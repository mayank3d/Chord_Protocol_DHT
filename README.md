# Chord_Protocol_DHT
Chord DHT Protocol implementation in Scala using Akka Actor model

A Scala and Akka implementation of Chord protocol for lookup in DHT network. 
Allows user to decide number of nodes and number of message request made by each node.
Each node will make 1 lookup request per second untill it has made required number of requests.

How to run:
You can use sbt build config file.
Go to the directory Chord and type "sbt"
Then enter command "run 1024 5"
Here 1024 is the number of nodes in the network and 5 is the request at each node.
You can use any values for node count and messages, however a number in power of two is recommended.
The program will display statistics while it is running and at termination it produces average number of hops each message has to travel to locate desired key in the DHT.

Happy Hashing !!
