#!/bin/sh
# Makes the PostgreSQL of a test one that takes TLS and nothing else (ADR-0013 §5).
#
# Run by the image's entrypoint when the data directory is made, after the bootstrap scripts
# and before the server starts for good. It makes a certificate authority of the test's own,
# has it sign a certificate for the server, and leaves no way in over TCP that is not TLS.
#
#   TLS_SUBJECT_ALT_NAMES  the names the server is reached by, as DNS:localhost,IP:127.0.0.1
set -eu

cd "$PGDATA"

# The authority. Its key is deleted below: nothing else is ever signed by it.
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=Test authority of the lakehouse" \
  -keyout ca.key -out ca.crt 2>/dev/null

# The server's certificate, for the names it is reached by and no other.
openssl req -newkey rsa:2048 -nodes -subj "/CN=postgres" -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=%s\nbasicConstraints=CA:FALSE\n' "$TLS_SUBJECT_ALT_NAMES" >server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 2 \
  -extfile server.ext -out server.crt 2>/dev/null
chmod 600 server.key
rm -f ca.key ca.srl server.csr server.ext

# server.crt and server.key in the data directory are the server's defaults.
echo "ssl = on" >>postgresql.conf

# Every way in over TCP becomes one over TLS. A connection without it then matches no line,
# and is refused before it is asked for a password.
sed -i -E 's/^host[[:space:]]+/hostssl /' pg_hba.conf
if grep -qE '^host(nossl)?[[:space:]]' pg_hba.conf; then
  echo "pg_hba.conf still lets a connection in without TLS" >&2
  exit 1
fi
