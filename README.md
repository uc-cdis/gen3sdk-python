# Gen3 SDK for Python

The Gen3 SDK for Python provides classes for handling the authentication flow using a refresh token and getting an access token from the commons. The access token is then refreshed as necessary while the refresh token remains valid. The submission client contains various functions for submitting, exporting, and deleting data from a Gen3 data commons.

Docs for this SDK are available at [http://gen3sdk-python.rtfd.io/](http://gen3sdk-python.rtfd.io/)

## Auth

This contains an auth wrapper for supporting JWT based authentication with `requests`. The access token is generated from the refresh token and is regenerated on expiration.

## IndexClient

This is the client for interacting with the Indexd service for GUID brokering and resolution.

## SubmissionClient

This is the client for interacting with the Gen3 submission service including GraphQL queries.

