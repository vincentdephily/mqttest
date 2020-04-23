# 0.3.0 (unreleased)

* You can now send commands to a running server
  - Send an `mqttrs::Packet` to a client
  - Disconnect a client
  - Stop the server
* You can now receive events from the server
  - Client connect/disconnect
  - Packets sent/received
  - Server termination
* Events can be filtered via `Conf::event_*()` builders
* Waiting for the server to finish is now done via `Mqttest::finish()` method
  - This returns some basic runtime statistics

MSRV goes up to 1.42 for running `mqttest`'s own tests, but remains at 1.39 for building and using
in your own unittests.

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
