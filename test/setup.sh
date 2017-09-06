#!/bin/bash

set -eo pipefail

DIRNAME=$1
RAMNAME=eddy-$USER

[ -d $DIRNAME ] && exit 0
rm -rf $DIRNAME || exit 1

if which hdiutil > /dev/null; then
	if [ -d /Volumes/$RAMNAME ]; then
		ln -s /Volumes/$RAMNAME $DIRNAME && exit 0
	elif DEVNAME=$(hdiutil attach -nomount ram://2097152); then
		if diskutil erasevolume HFS+ $RAMNAME $DEVNAME; then
			ln -s /Volumes/$RAMNAME $DIRNAME && exit 0
			diskutil unmount force $DEVNAME
		else
			hdiutil detach $DEVNAME
		fi
	fi
fi

if [ -d /dev/shm ]; then
	if [ -d /dev/shm/$RAMNAME ]; then
		ln -s /dev/shm/$RAMNAME $DIRNAME && exit 0
	elif mkdir /dev/shm/$RAMNAME && ln -s /dev/shm/$RAMNAME $DIRNAME; then
		exit 0
	fi
fi

mkdir $DIRNAME
