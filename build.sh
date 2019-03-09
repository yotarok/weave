#!/bin/sh

sbt "project weaveCore" assembly doc
sbt "project weaveSchd" assembly doc
#sbt "project sampleRecipe" assembly doc

sbt "project weaveStdLib" publishLocal
sbt publishLocal

./install.sh bin
