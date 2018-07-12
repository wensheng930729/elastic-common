projVersion=${CIRCLE_TAG:1}
signing.keyId=${GPG_KEYID}
signing.password=${GPG_PASS}
signing.secretKeyRingFile=secring.gpg

ossrhUsername=${NEXUS_USER}
ossrhPassword=${NEXUS_PASS}