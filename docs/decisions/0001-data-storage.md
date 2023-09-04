# 1. Data storage

Date: 2023-09-04

## Status

Accepted

## Context

The main goal of a vector database is to provide efficient and reliable vector search. In many applications, specially those related to Machine Learning, such vectors represent underlying data that could be text, image, audio, video or something else. Should Vectoria store this data?

## Decision

We will not store any data that a vector is potentially mapped to. Only the vector and related metadata will be stored.
Rationale:
- The diversity of the data would impose difficulties to reliably storing and retrieving it;
- Storage decisions would limit usage due to trade-offs;
- We would loose focus on vector retrieval, which is the main goal of the project.

## Consequences

- The user will be responsible for storing the underlying data;
- Each vector will require a unique identifier given by the user (that's how the mapping occurs);
- Vectoria remains agnostic to the type of data, allowing for more use cases.

## Participants

- [@caiodallaqua](https://github.com/caiodallaqua)