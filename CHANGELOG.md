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
