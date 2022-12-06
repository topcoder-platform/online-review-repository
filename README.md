# Online Review Persistence Calls Repository

This is a simple gRPC service that implements the [OR Persistence Calls](https://github.com/topcoder-platform/online-review-interface-definition/tree/main/src/main/proto) interfaces

# Note

This is in the early stages of development and while it is functional and the interfaces are fully defined, it is not yet ready for production use.

# Local Development

1. Make sure you have [tc-dal-or-proto](https://github.com/topcoder-platform/online-review-interface-definition) installed on your local maven repository
2. Make sure you have Informix running locally or on a remote server and that you can connect to it.
3. Set the following environment variables:
    - `DB_URL`: The connection String (example "DB_URL=jdbc:informix-sqli://localhost:8877/tcs_catalog:INFORMIXSERVER=informixoltp_tcp;IFX_LOCK_MODE_WAIT=5;OPTCOMPIND=0;STMT_CACHE=1;DB_USERNAME=USERNAME;DB_PASSWORD=PASSWORD")
