# Nifi OPC-UA Bundle (Mondi Group Fork)

> **Important notice**  
> This project is a fork and major modification of the
> [linksmart/nifi-opc-ua-bundles](https://github.com/linksmart/nifi-opc-ua-bundles)
> project, originally developed by the Fraunhofer Institute for Applied
> Information Technology FIT and published under the Apache License, Version 2.0.

This repository contains an OPC UA controller service and processors for
Apache NiFi.  
It is based on the original Nifi OPC-UA Bundle developed by
Fraunhofer FIT and maintained by LinkSmart, and has been adapted and extended
by **Mondi Group** to meet current technical and operational requirements.

## Background

The original project is an improvement built on top of the OPC UA bundle made
by [HashmapInc](https://github.com/hashmapinc/nifi-opcua-bundle).

The LinkSmart repository is currently archived and not actively maintained.
This fork continues development internally at Mondi Group, including
modernization and dependency upgrades.

## Key Differences Compared to the Original Bundle

Compared to the original `HashmapInc` and LinkSmart versions, this fork provides:

1. Migration from legacy Eclipse Milo versions to **Eclipse Milo 1.1.1**
2. Updated build and dependency configuration
3. Internal code refactoring and package namespace changes
4. Improvements to security, error handling and operational stability
5. Adaptations for modern Apache NiFi versions and enterprise use cases

The foundational architecture remains the same:

- The original `HashmapInc` bundle is based on the
  [OPC UA-Java Stack](https://github.com/OPCFoundation/UA-Java)
- This bundle is based on [Eclipse Milo](https://github.com/eclipse/milo),
  which provides higher-level OPC UA client APIs and advanced functionality
  such as subscriptions.

## Build Instructions

### Build and install the NAR manually

mvn clean install -DskipTests

## Contributing
Contributions are welcome in terms of documentation, implementations, and technical support.
Please fork, make your changes, and submit a pull request. For major changes, please open an issue first and discuss it with the other authors.
