#!/bin/bash -xe

TDIR=/tmp/build

mvn package
rm -rf $TDIR
mkdir -p $TDIR
HOME=$TDIR rpmdev-setuptree
sed -i "s/\%(echo \$HOME)/\/tmp\/build/g" $TDIR/.rpmmacros

cp target/taskworker-core.tar.bz2 $TDIR/rpmbuild/SOURCES/
cp taskworker-core.spec $TDIR/rpmbuild/SPECS/taskworker-core.spec

rpmbuild -D "%_topdir $TDIR/rpmbuild" -bb $TDIR/rpmbuild/SPECS/taskworker-core.spec

if [[ "$1" != "" ]]; then
    cp $TDIR/rpmbuild/RPMS/noarch/*.rpm $1
    rm -rf $TDIR
fi
