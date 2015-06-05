eventsource-io
=============

a tool for the command line interface to read eventstore streams into json files, or specially formatted json files into eventstore streams

download a .NET 4.5 (client profile) binary from the master source tree here:

https://github.com/j-alexander/eventsource-io/blob/master/EventSourceIO.exe?raw=true


```

USAGE: EventSourceIO.exe source target

      source/target can be:

      --json=[path-to-file.json]
      --gzip=[path-to-compressed-file.json.gz]
      --kafka=[host[:port][,host2[:port2][,host3[:port3][...]]]]/topic
      --eventstore=[username[:password]@]hostname[:port][/stream_name[+start]]

            e.g. admin:changeit@localhost:1113/destination-a31613b3a13

 import a json file:
      EventSourceIO.exe --json=stream-a31613b3e13.json --eventstore=localhost

 import a json file to a specific stream:
      EventSourceIO.exe --json=stream-a31613b3e13.json --eventstore=admin:changeit@localhost:1113/copied-a31613b3a13

 export all to a json file:
      EventSourceIO.exe --eventstore=localhost --json=exported.json

 export a specific stream from localhost to file:
      EventSourceIO.exe --eventstore=/source_stream --json=exported.json
	  
```

events are stored in minimized json, new-line separated, according to the following format:
```JSON
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
{ "type":"eventtype", "stream":"eventstream", "data": {}, "metadata": {} }
...
```
data and metadata fields are inline, json-formatted, in order to facilitate simple editing
