#!/bin/sh

mkdir -p $1
echo Installing $1/weave
./mkscript.sh ./core/target/scala-2.11/weaveCore-assembly-0.1-SNAPSHOT.jar $1/weave
echo Installing $1/weaveschd
./mkscript.sh ./schd/target/scala-2.11/weaveSchd-assembly-0.1-SNAPSHOT.jar $1/weaveschd
