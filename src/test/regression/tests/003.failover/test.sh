#!/usr/bin/env bash
#-------------------------------------------------------------------
# test script for failover
#
source $TESTLIBS
TESTDIR=testdir
PG_CTL=$PGBIN/pg_ctl
PSQL="$PGBIN/psql -X "

for mode in s r
do
	rm -fr $TESTDIR
	mkdir $TESTDIR
	cd $TESTDIR

# create test environment
	echo -n "creating test environment..."
	$PGPOOL_SETUP -m $mode -n 2 --no-stop|| exit 1
	echo "done."

	source ./bashrc.ports

	export PGPORT=$PGPOOL_PORT

	$PSQL -c "show pool_nodes" test

	# trrigger failover
	$PG_CTL -D data1 -m f stop
	wait_for_pgpool_startup
	$PSQL -c "show pool_nodes" test > result

	# check the output of "show pool_nodes".
	LANG=C $PSQL -f ../create_expected.sql -v mode="'$mode'" -v dir="'$PGSOCKET_DIR'" test | tail -n 6 > expected
	cmp result expected > /dev/null 2>&1
	if [ $? != 0 ];then
		./shutdownall
		exit 1
	fi
		
	./shutdownall

	cd ..

done

exit 0
