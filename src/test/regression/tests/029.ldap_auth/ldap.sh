#!/usr/bin/env bash

# $ slappasswd -s password     
# {SSHA}+VP7XPZZ5lN6lMw648UEZQoPW3eqtJ1s

# $ slappasswd -s ldap_password
# {SSHA}GnaMm5qxWjf4qgzn6CD6Pllwj/pvudTy


MANAGERPWD=passowrd

# add manager

# add ldap_user
ldap ldapadd -x -D "cn=Manager,dc=example,dc=com" -w $MANAGERPWD -f add_user.ldif


