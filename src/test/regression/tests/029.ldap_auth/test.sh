#!/usr/bin/env bash
#-------------------------------------------------------------------
# test script for LDAP authentication for: frontend <--> Pgpool-II.
#
source $TESTLIBS
TESTDIR=testdir
PSQL=$PGBIN/psql
PG_CTL=$PGBIN/pg_ctl
export PGDATABASE=test

# Generate LDAP setting
# sh ldap.sh

dir=`pwd`

rm -fr $TESTDIR
mkdir $TESTDIR
cd $TESTDIR

# create test environment. Number of backend node is 1 is enough.
echo -n "creating test environment..."
$PGPOOL_SETUP -m s -n 1 || exit 1
echo "done."

dir=`pwd`

echo "enable_pool_hba = on" >> etc/pgpool.conf
echo "log_connections = on" >> etc/pgpool.conf

# allow to access IPv4 localhost
#echo "local	all		all					ldap ldapserver=localhost ldapprefix=\"cn=\" ldapsuffix=\",dc=example,dc=com\"" >> etc/pool_hba.conf

# simple bind
echo "host	all		ldap_user	127.0.0.1/32	ldap ldapserver=localhost ldapprefix=\"cn=\" ldapsuffix=\",dc=example,dc=com\" ldappassthroughauth=1" >> etc/pool_hba.conf
echo "host	all		ldap_user	::1/128			ldap ldapserver=localhost ldapprefix=\"cn=\" ldapsuffix=\",dc=example,dc=com\" ldappassthroughauth=1" >> etc/pool_hba.conf
# search and bind
echo "local  all     ldap_user	ldap ldapserver=localhost ldapbinddn=\"cn=Manager,dc=example,dc=com\" ldapbindpasswd=\"password\" ldapbasedn=\"dc=example,dc=com\" ldapsearchfilter=\"(cn=\$username)\" ldappassthroughauth=1" >> etc/pool_hba.conf


sed -i "/.*trust$/d" etc/pool_hba.conf
echo "local   all         all                               trust" >> etc/pool_hba.conf

# allow to access localhost from ldap_user
sed -i "/replication/!s/^local.*trust$//g" data0/pg_hba.conf
echo "local		all		ldap_user	scram-sha-256" >> data0/pg_hba.conf
echo "local		all		all			trust" >> data0/pg_hba.conf

#echo "ldap_user:ldap_password" >> etc/pool_passwd


source ./bashrc.ports

./startall

export PGPORT=$PGPOOL_PORT

wait_for_pgpool_startup

export PGPASSWORD=ldap_password

$PSQL -c "SET password_encryption = 'scram-sha-256'; CREATE ROLE ldap_user PASSWORD 'ldap_password'; ALTER ROLE ldap_user WITH LOGIN;" test


$PSQL -h localhost -c "select 1" test -U ldap_user

if [ $? != 0 ];then
	echo "ldap simple bind authenticaion is failed"
    ./shutdownall
	exit 1
fi

$PSQL  -c "select 1" test -U ldap_user

if [ $? != 0 ];then
	echo "ldap simple bind authenticaion is failed"
    ./shutdownall
	exit 1
fi

echo "Checking LDAP auth between Pgpool-II and frontend was ok."

./shutdownall
exit 0

