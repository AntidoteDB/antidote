#!/usr/bin/env bash

# just bail out if things go south
set -e

: ${RTEE_DEST_DIR:="$HOME/rt/antidote"}

cd "$( dirname "${BASH_SOURCE[0]}" )/../../"
ANTIDOTE_DIR=$(pwd)

echo "Making $ANTIDOTE_DIR the current release:"
echo -n " - Determining version: "
VERSION="current"

# Uncomment this code to enable tag trackig and versioning.
#
# if [ -f dependency_manifest.git ]; then
#     VERSION=`cat dependency_manifest.git | awk '/^-/ { print $NF }'`
# else
#     VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
# fi

echo $VERSION

echo " - Creating $HOME/.riak_test.config"
cp -i riak_test/bin/.riak_test.config $HOME && \
sed -i -e "s#TESTS_PATH#$ANTIDOTE_DIR\/riak_test#g" -e "s#RTDEV_PATH#$RTEE_DEST_DIR#g" $HOME/.riak_test.config

mkdir -p $RTEE_DEST_DIR
cd $RTEE_DEST_DIR
echo " - Configure $RTEE_DEST_DIR"
git init . > /dev/null 2>&1

echo " - Creating $RTEE_DEST_DIR/current"
mkdir current

echo " - Copying devrel to $RTEE_DEST_DIR/current"
cp -p -P -R $ANTIDOTE_DIR/dev current
echo " - Writing $RTEE_DEST_DIR/current/VERSION"
echo -n $VERSION > current/VERSION

echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" > /dev/null 2>&1
