# 0.3.0 (unreleased)

* You can now send commands to a running server
  - Send an `mqttrs::Packet` to a client
  - Disconnect a client
  - Stop the server
* You can now receive events from the server
  - Client connect/disconnect
  - Packets sent/received
  - Server termination
  - Types received can be filtered via `Conf::event_*()` builders
  - The server will buffer a finite but configurable number of events
  - Events can be received async during the server run, or sync at server finish
* Waiting for the server to finish is now done via `Mqttest::finish()` method
  - This returns some basic runtime statistics, and any leftover events
* Added a max_runtime config to stop the server after a delay

# 0.2.0 (2020-04-06)

Cater for the library usecase.

* Changed server start/await API to be more practical
* Revamped docs
* Added example unittest
* Improved Conf API ergonomy
* Added a dump_prefix config
* Switched mqttrs to upstream/crates.io release
* Updated tokio-util to 0.3

There is still plenty to do, but these changes were enough to convert my unittest to call the
library instead of running the binary.

# 0.1.0 (2020-03-03)

First release :)

Mqttest is at this stage a niffty unittesting tool, even if some features are still missing. My
short-term plan is to work on the library usecase (as opposed to running a binary). Now is a great
time to chime in with your usecase, bug report, or code contribution.
