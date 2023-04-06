PRESTO_HOME=$HOME/p/presto-server-0.280
PRESTO_DEV=$HOME/p/presto-kite

$PRESTO_HOME/bin/launcher stop
mkdir -p $PRESTO_HOME/plugin/kite
cp $PRESTO_DEV/target/presto-kite-0.280/* $PRESTO_HOME/plugin/kite
$PRESTO_HOME/bin/launcher start

