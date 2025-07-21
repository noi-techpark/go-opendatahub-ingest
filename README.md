<!--
SPDX-FileCopyrightText: 2024 NOI Techpark <digital@noi.bz.it>

SPDX-License-Identifier: CC0-1.0
-->


# DEPRECATED
This repo is deprecated, all service should use https://github.com/noi-techpark/opendatahub-go-sdk instead


# Golang libs for Open Data Hub ingestion microservices

These libraries are for use inside the Open Data Hub to implement Data collectors, Transformers and Elaborations

## dc
Data collector specific utilities. Services that push to a rabbitmq exchange

## tr
Transformer specific utilities. Services that listen on a rabbitmq queue

## dto
Data transfer objects for message exchange and raw data storage

## mq
Simplified interface for rabbitmq

## ms (microservices)
General utility boilerplate like logging, configuration and error handling

