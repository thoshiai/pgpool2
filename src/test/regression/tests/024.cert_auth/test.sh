#!/usr/bin/env bash
#-------------------------------------------------------------------
# test script for cert authentication for: frontend <--> Pgpool-II.
#
source $TESTLIBS
TESTDIR=testdir
PSQL=$PGBIN/psql
PG_CTL=$PGBIN/pg_ctl
export PGDATABASE=test

# Generate certifications
./cert.sh

dir=`pwd`
SSL_KEY=$dir/server.key
SSL_CRT=$dir/server.crt
SSL_CRL=$dir/server.crl
ROOT_CRT=$dir/root.crt
FRONTEND_KEY=$dir/frontend.key
FRONTEND_CRT=$dir/frontend.crt
chmod 600 *.key

rm -fr $TESTDIR
mkdir $TESTDIR
cd $TESTDIR

# create test environment. Number of backend node is 1 is enough.
echo -n "creating test environment..."
$PGPOOL_SETUP -m s -n 1 || exit 1
echo "done."

dir=`pwd`

echo "ssl = on" >> etc/pgpool.conf
echo "ssl_key = '$SSL_KEY'" >> etc/pgpool.conf
echo "ssl_cert = '$SSL_CRT'" >> etc/pgpool.conf
echo "ssl_ca_cert = '$ROOT_CRT'" >> etc/pgpool.conf
echo "enable_pool_hba = on" >> etc/pgpool.conf
echo "listen_address = '*'"  >> etc/pgpool.conf

#echo "ssl_prefer_server_ciphers = on" >> etc/pgpool.conf
#echo "ssl_ciphers = 'EECDH:HIGH:MEDIUM:+3DES:!aNULL'" >> etc/pgpool.conf

# allow to access IPv6 localhost
echo "hostssl	all	    all		127.0.0.1/32          cert" >> etc/pool_hba.conf
echo "hostssl	all	    all		::1/128          cert" >> etc/pool_hba.conf

sed -i "/^host.*trust$/d" etc/pool_hba.conf

source ./bashrc.ports

./startall

export PGPORT=$PGPOOL_PORT

wait_for_pgpool_startup

export PGSSLCERT=$FRONTEND_CRT
export PGSSLKEY=$FRONTEND_KEY
export PGSSLROOTCERT=$ROOT_CRT

$PSQL -h localhost -c "select 1" test

grep "SSL certificate authentication for user" log/pgpool.log|grep successful
if [ $? != 0 ];then
    echo "Checking cert auth between Pgpool-II and frontend failed."
    ./shutdownall
    exit 1
fi

echo "Checking cert auth between Pgpool-II and frontend was ok."

./shutdownall


# Starting CRL verfication
# Adding valid CRL file in pgpool.conf file.
echo "ssl_crl_file = '$SSL_CRL'" >> etc/pgpool.conf

# Check pgpool configuration is updated successfully
grep "server.crl" etc/pgpool.conf
if [ $? != 0 ];then
    echo "pgpool.conf is not updated with CRL file."
    ./shutdownall
    exit 1
fi

# Start Server and PgPool
./startall

export PGPORT=$PGPOOL_PORT

wait_for_pgpool_startup

export PGSSLCERT=$FRONTEND_CRT
export PGSSLKEY=$FRONTEND_KEY
export PGSSLROOTCERT=$ROOT_CRT

$PSQL -h localhost -c "select 1" test

grep "SSL certificate authentication for user" log/pgpool.log|grep successful
if [ $? != 0 ];then
    echo "Checking cert auth between Pgpool-II and frontend with clean CRL failed."
    ./shutdownall
    exit 1
fi

echo "Checking cert auth between Pgpool-II and frontend with clean CRL was ok."

./shutdownall


# Adding CRL file with revoked certification entry in pgpool.conf file.
echo "Updating pgpool.conf with revoked CRL file"

sed -i 's/server.crl/server_revoked.crl/' etc/pgpool.conf

# Check pgpool configuration is updated successfully
grep "server_revoked.crl" etc/pgpool.conf
if [ $? != 0 ];then
    echo "pgpool.conf is not updated with revoked CRL file."
    ./shutdownall
    exit 1
fi

# Start Server and PgPool
./startall

export PGPORT=$PGPOOL_PORT

wait_for_pgpool_startup

export PGSSLCERT=$FRONTEND_CRT
export PGSSLKEY=$FRONTEND_KEY
export PGSSLROOTCERT=$ROOT_CRT

$PSQL -h localhost -c "select 1" test > $dir/crl_session.log  2>&1

grep "sslv3 alert certificate revoked" $dir/crl_session.log

if [ $? != 0 ];then
    echo "Checking cert auth between Pgpool-II and frontend with revoked entry in CRL failed."
    ./shutdownall
    exit 1
fi

echo "Checking cert auth between Pgpool-II and frontend with revoked entry in CRL was ok."

./shutdownall

exit 0
