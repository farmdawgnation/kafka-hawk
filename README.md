# kafka-hawk

An application that records stats about consumer group offset commits and
reports them as prometheus metrics.

This application is useful for determining how frequently each consumer group
in your Kafka cluster is committing the offsets and, for the chattiest ones,
it can calculate what the offset deltas are over time.

## Building

If you wish to build the project you'll need:

* JDK 11

## Deployment

This application is packaged and designed to be deployed using a Docker image.
It's available on Docker Hub as `farmdawgnation/kafka-hawk`.

## Configuration

The application can be configured by environment variables for most cases.

* `BOOTSTRAP_SERVERS` — (required) The bootstrap servers setting for hawk
* `GROUP_ID` - (optional) The group id, defaults to "hawk"
* `SASL_CONFIG` - (optional) The SASL config if needed

The application also has an optional feature that can report on the offset
commit deltas. You have to enable this per consumer group because it requires
more resources to track the offset diffs in memory. It can be enabled with
and configured with the following environment variables:

* `FEATURES_DELTAS_ENABLED` - Set to "true" to enable the feature
* `FEATURES_DELTAS_GROUPS` - A comma separated list of groups to report deltas on

# About the Author

This tool was written by Matt Farmer who by day slings code and architecture
for [Mailchimp](https://mailchimp.com)'s Data Application Development Team.
