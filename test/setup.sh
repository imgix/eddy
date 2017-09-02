#!/bin/bash

set -eo pipefail

RAMNAME=eddy-$USER
TMPNAME=/tmp/eddy

[ -d $TMPNAME ] && exit 0
rm -rf $TMPNAME || exit 1

if which hdiutil; then
	if [ -d /Volumes/$RAMNAME ]; then
		ln -s /Volumes/$RAMNAME $TMPNAME && exit 0
	elif DEVNAME=$(hdiutil attach -nomount ram://2097152); then
		if diskutil erasevolume HFS+ $RAMNAME $DEVNAME; then
			ln -s /Volumes/$RAMNAME $TMPNAME && exit 0
			diskutil unmount force $DEVNAME
		else
			hdiutil detach $DEVNAME
		fi
	fi
fi

if [ -d /dev/shm ]; then
	if [ -d /dev/shm/$RAMNAME ]; then
		ln -s /dev/shm/$RAMNAME $TMPNAME && exit 0
	elif mkdir /dev/shm/$RAMNAME && ln -s /dev/shm/$RAMNAME $TMPNAME; then
		exit 0
	fi
fi

mkdir $TMPNAME
