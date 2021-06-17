# Release Notes

## Changes in Grid 0.1.3

### Bug Fix

This release fixes a bug with transaction creation for Grid Location in the Grid
CLI. Previously, the Grid Schema namespace was not being added to the input
addresses of 'LocationCreateAction' transactions, causing them to be invalid.
This issue has now been fixed.

## Changes in Grid 0.1.2

### Overview

This release reorganizes and updates some features in Grid. With this release,
we are able to run a working version of Grid without using any experimental
features.

### Grid CLI

Moved the following features from "experimental" to "default":
 - `splinter`
 - `sqlite`

These features are now stable in Grid 0.1.2.

Moved the following features from "stable" to "default":
 - `location`
 - `pike`
 - `product`
 - `schema`

### Grid Daemon

The Grid daemon now requires the stable version of the Grid SDK instead of
experimental by default. `track-and-trace` functionality in gridd was moved
behind a feature which depends on the `grid-sdk/track-and-trace` feature, since
the track and trace SDK feature is not part of the stable API.

Moved the following features from "stable" to "default":
 - `pike`
 - `schema`
 - `product`
 - `location`
 - `splinter-support`

### Grid SDK

`stable` was removed from the `database` feature, since the features in
`database` are already included in `stable`.

## Changes in Grid 0.1

### Highlights

As the initial release of Grid, the following release notes list all of the
stable and experimental features present in the release.

### Stable

#### Location (Smart Contract)

Grid Location is a framework for sharing location master data between trade
partners. The framework is built in a generic and extensible way to allow for
flexibility in serving various use cases and specialized industries. The first
extension of this framework – a GS1 compliant location – is built and allows
organizations to harness the power of a widely adopted industry standard.
Location is a universal concept within the supply chain and is naturally one of
the highest areas of re-use across Grid applications.

#### Pike (Smart Contract)

Pike is designed to track the identities of the actors involved in the supply
chain. These actors are agents and the organizations they represent. The roles
that the agents play within said organizations are also tracked. This
information can be used to determine who is allowed to interact with a platform,
and to what extent they are allowed to interact with the platform.

#### Sawtooth Support

Hyperledger Grid supports Sawtooth as a backend distributed ledger. When used
in this way, the Sawtooth validator runs alongside a Sabre transaction
processor on each node. The Grid smart contracts are then uploaded to the
blockchain state via Sabre transactions.

#### Schema

Schema provides a reusable, standard approach to defining, storing, and
consuming properties within smart contracts, software libraries, and
network-based APIs.

### Experimental

#### Grid Database Support

Hyperledger Grid can support either PostgreSQL or Sqlite as a database back end.

#### Splinter Support

Hyperledger Grid supports Splinter's Scabbard service as a backend distributed
ledger. When used in this way, the Grid daemon connects with a Splinter daemon
on each Grid node. The Grid daemon will automatically set up newly created
Splinter circuits with the "grid" management type by uploading the Grid smart
contracts to Scabbard state.
