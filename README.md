MapReduce
=========

f0r 15440

1. Overview
----------------------------------------------------------------------------
Implementation of a simple map-reduce distributed system. Client and servers 
communicate with XML syntax and HTTP for error reporting. 
PS. the project directory is refered to as "./" in this report.

2. Components
----------------------------------------------------------------------------
i> ./setup.sh is used as a control script that takes care of compiling and 
running all the parts needed for the map-reduce system.

ii>./config.txt is used as a configuration file that has the following entries:
    a) nameNodePort: the port the name node uses to listen to client requests
    b) dataNodePort: the port the data node uses to listen to name node
    c) nameNodeIP: unique, the ip address of the name node
    d) dataNodeIP: not unique, the ip address(es) of data node(s)

iii> source/ contains source code for the server, client and tools.
source/tools/ has the following useful tools:
    a) a parser and a printer: used to generate the messages
    b) Mapper and Reducer Interfaces
    c) Base64Coder: used to encode an object in base64 for transmission

iv> source/client provides the following builtin testing objects:
    a) count: counts the number of words in data/book.txt
    b) 
    c)

3. Testing
----------------------------------------------------------------------------
To test with a builtin client object, do the following: 
    i>   ./setup make
    ii>  on a data node run ./setup data
    iii> on a name node run ./setup name
    iv>  on a client, run ./setup client and then choose the builtin object

To test with a custom object, you need to implement the following:
    i>   a mapper ascribing to Mapper interface
    ii>  a reducer ascribing to Reducer interface
    iii> a runnable file that instantiates your mapper and reducer as well 
    as the MapReduceClient object for contacting the name node

4. Features
----------------------------------------------------------------------------
i> The servers (name node and data nodes) do not need to have an implementation
of the mapper or reducer. They are all encoded as part of the message.

ii> The clients will upload the data beforehand and the name node will 
dynamically split up the data and send to all the participating data nodes.

iii> Currently supports operation on integer, boolean and string. 

iv> Extensive logging records each and every message according to its time
of receipt. 

5. Design
----------------------------------------------------------------------------
After the name node and data nodes are up and found each other according to the
information in ./config.txt, the clients can start sending 'job requests' to 
name node. The name node will split up the mapper and reducer and store them
locally. Then the name node chooses a subset of all the available data nodes 
as the mapper group. It will send a 'mapper request' to each node in this group.
After every mapper node replies with the local answer, the name node picks a 
reducer and send it the mapper responses and the reducer object as if it is a
RPC call. After the reducer returns with an answer, the name node forwards the 
response back to the client. Then the client will be happy. 

The scheduling policies are not very intelligent but has room to improve if I
had the time. Basically right now I keep an array that records how many jobs 
each data node has and only pick the lowest n for mappers and the lowest one 
for reducer. 

6. Future work
----------------------------------------------------------------------------
Aside from the scheduling policy described above, the system needs to be 
improved in the following ways:
    a) Security: The data node will just run whatever the object it gets from
    the name node. A malicious client could upload a destructive object and 
    cause the server to segfault or memory dump.

    b) 


