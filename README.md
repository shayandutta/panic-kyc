# KYC Platform

A small identity verification platform written in Go.

Clients call a REST API to verify a PAN, either one at a time or in bulk.
The platform checks several upstream data sources with fallback, stores an
audit trail, and delivers results to the client by signed webhook.

Work in progress.
